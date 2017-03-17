package forklift.connectors;

import forklift.message.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Manages the {@link org.apache.kafka.clients.consumer.KafkaConsumer} thread.  Polled records are sent to the MessageStream. Commits
 * are batched and performed on any {@link #acknowledge(org.apache.kafka.clients.consumer.ConsumerRecord) acknowledged records}.  Best
 * performance is usually achieved using one controller instance for all topic subscriptions.
 * <p>
 * <strong>WARNING: </strong>Kafka does not lend itself well to message level commits.  For this reason, the controller sends commits
 * as a batch once every poll cycle.  It should be noted that it is possible for a message to be
 * processed twice should an error occur after the acknowledgement and processing of a message but before the commit takes place.
 */
public class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);
    private volatile boolean running = false;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaConsumer<?, ?> kafkaConsumer;
    private final MessageStream messageStream;
    private volatile boolean topicsChanged = false;
    private Object topicsMonitor = new Object();
    private AcknowledgedRecordHandler acknowlegmentHandler = new AcknowledgedRecordHandler();
    private String controllerName;
    private Map<TopicPartition, OffsetAndMetadata> failedOffset = null;
    private Map<TopicPartition, AtomicInteger> flowControl = new ConcurrentHashMap<>();

    public KafkaController(KafkaConsumer<?, ?> kafkaConsumer, MessageStream messageStream, String controllerName) {
        this.kafkaConsumer = kafkaConsumer;
        this.messageStream = messageStream;
        this.controllerName = controllerName;
    }

    /**
     * Adds a topic which the underlying {@link org.apache.kafka.clients.consumer.KafkaConsumer} will be subscribed to.  Adds
     * the topic to the messageStream.
     *
     * @param topic the topic to subscribe to
     * @return true if the topic was added, false if already added
     */
    public synchronized boolean addTopic(String topic) {
        if (!topics.contains(topic)) {
            messageStream.addTopic(topic);
            topics.add(topic);
            topicsChanged = true;
            synchronized (topicsMonitor) {
                topicsMonitor.notify();
            }
            return true;
        }
        return false;
    }

    /**
     * Unsubscribes the underlying {@link org.apache.kafka.clients.consumer.KafkaConsumer} from the passed in topic and removes the
     * topic from the message stream.
     *
     * @param topic the topic to remove
     * @return true if the topic was removed, false if it wasn't present
     */
    public synchronized boolean removeTopic(String topic) {
        boolean removed = topics.remove(topic);
        if (removed) {
            messageStream.removeTopic(topic);
            topicsChanged = true;
        }
        return removed;
    }

    /**
     * Whether or not this service is running.
     *
     * @return true if this service is running, else false
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Acknowledge that processing is beginning on a record.  True is returned if this controller is still managing the
     * partition the record originated from.  If the partition is no longer owned by this controller, false is returned
     * and the message should not be processed.
     *
     * @param record the record to acknowledge
     * @return true if the record should be processed, else false
     * @throws InterruptedException
     */
    public boolean acknowledge(ConsumerRecord<?, ?> record) throws InterruptedException {
        AtomicInteger flowCount = flowControl.get(new TopicPartition(record.topic(), record.partition()));
        if(flowCount != null){
            flowCount.decrementAndGet();
        }
        log.debug("Acknowledge message with topic {} partition {} offset {}", record.topic(), record.partition(), record.offset());
        return running && this.acknowlegmentHandler.acknowledgeRecord(record);
    }

    /**
     * Starts the controller.  Must be called before any other methods.
     * <p>
     * <strong>WARNING: </strong>Avoid starting a service which has already run and been stopped.  No attempt is made
     * to recover a stopped controller.
     */
    public void start() {
        running = true;
        executor.submit(() -> controlLoop());
    }

    /**
     * Stops the controller.
     *
     * @param timeout  the time to wait for the service to stop
     * @param timeUnit the TimeUnit of timeout
     * @throws InterruptedException
     */
    public void stop(long timeout, TimeUnit timeUnit) throws InterruptedException {
        executor.shutdownNow();
        kafkaConsumer.wakeup();
        running = false;
        executor.awaitTermination(timeout, timeUnit);
    }

    private void controlLoop() {
        try {
            while (running) {
                boolean updatedAssignment = false;
                if (topics.size() == 0) {
                    //check if the last remaining topic was removed
                    if (kafkaConsumer.assignment().size() > 0) {
                        Map<TopicPartition, OffsetAndMetadata>
                                        offsetData =
                                        this.acknowlegmentHandler.removePartitions(kafkaConsumer.assignment());
                        failedOffset = offsetData;
                        if (offsetData.size() > 0) {
                            log.info(controllerName + "topics changed CommitSync before unsubscribe " + offsetData.size());
                        }
                        kafkaConsumer.commitSync(offsetData);
                        failedOffset = null;
                        kafkaConsumer.unsubscribe();
                    }
                    synchronized (topicsMonitor) {
                        //recheck wait condition inside synchronized block
                        while (topics.size() == 0) {
                            //pause the polling thread until a topic comes in
                            log.debug("Control-Loop waiting for topic");
                            topicsMonitor.wait();
                        }
                    }
                }
                if (topicsChanged) {
                    Set<TopicPartition> removed = new HashSet<>();
                    synchronized (this) {
                        for (TopicPartition partition : kafkaConsumer.assignment()) {
                            if (!topics.contains(partition.topic())) {
                                removed.add(partition);
                            }
                        }
                        topicsChanged = false;
                    }
                    //commit any removed partitions before we unsubscribe them
                    Map<TopicPartition, OffsetAndMetadata> offsetData = this.acknowlegmentHandler.removePartitions(removed);
                    failedOffset = offsetData;
                    if (offsetData.size() > 0) {
                        log.info(controllerName + " topics changed committing offset: " + offsetData.size());
                    }
                    kafkaConsumer.commitSync(offsetData);
                    failedOffset = null;
                    kafkaConsumer.subscribe(topics, new RebalanceListener());
                    updatedAssignment = true;
                }

                log.debug("Control-Loop polling");

                //pause partitions that haven't processed yet
                Set<TopicPartition> paused = new HashSet<>();
                Set<TopicPartition> unpaused = new HashSet<>();
                for (Map.Entry<TopicPartition, AtomicInteger> entry : flowControl.entrySet()) {
                    if (entry.getValue().get() > 0) {
                        paused.add(entry.getKey());
                    } else {
                        unpaused.add(entry.getKey());
                    }
                }
                kafkaConsumer.pause(paused);
                kafkaConsumer.resume(unpaused);
                ConsumerRecords<?, ?> records = kafkaConsumer.poll(1000);
                if (updatedAssignment) {
                    this.acknowlegmentHandler.addPartitions(kafkaConsumer.assignment());
                    for (TopicPartition partition : kafkaConsumer.assignment()) {
                        this.flowControl.merge(partition, new AtomicInteger(),
                                               (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
                    }
                }
                if (records.count() > 0) {
                    log.info(controllerName + "Control-Loop adding: " + records.count() + " to record stream");
                    records.forEach(record -> flowControl.get(new TopicPartition(record.topic(), record.partition()))
                                                         .incrementAndGet());
                    messageStream.addRecords(consumerRecordsToKafkaMessages(records));
                }
                Map<TopicPartition, OffsetAndMetadata> offsetData = this.acknowlegmentHandler.flushAcknowledged();
                if (offsetData.size() > 0) {
                    String offsetDescription =
                                    offsetData.entrySet()
                                              .stream()
                                              .map(entry -> "topic: " + entry.getKey().topic() + ", " +
                                                            "partition: " + entry.getKey().partition() + ", " +
                                                            "offset: " + entry.getValue().offset())
                                              .collect(Collectors.joining("|"));
                    log.debug("Control-Loop Commiting offsets {}", offsetDescription);
                    failedOffset = offsetData;
                    kafkaConsumer.commitSync(offsetData);
                    failedOffset = null;
                }
            }
        } catch (WakeupException e) {
            log.info(controllerName + "Wakeup, Control-Loop exiting");
        } catch (InterruptedException e) {
            log.info(controllerName + "Interrupted, Control-Loop exiting");
        } catch (Throwable t) {
            log.error(controllerName + "Control-Loop error, exiting", t);
            throw t;
        } finally {
            running = false;
            try {
                Map<TopicPartition, OffsetAndMetadata>
                                offsetData =
                                this.acknowlegmentHandler.removePartitions(kafkaConsumer.assignment());
                try {
                    if (failedOffset != null) {
                        log.info(controllerName + "failedOffset of size: " + failedOffset.size());
                        for (TopicPartition partition : failedOffset.keySet()) {
                            offsetData.merge(partition, failedOffset.get(partition), (oldO, newO) -> {
                                if (oldO == null) {
                                    return newO;
                                }
                                if (newO == null) {
                                    return oldO;
                                }
                                return newO.offset() > oldO.offset() ? newO : oldO;
                            });
                        }
                    }

                    log.info(controllerName + "Closing offset size committed: " + offsetData.size());
                    try {
                        //if we got here through running = false or interrupt instead of wakeup, in this case wakeup hasn't been reset
                        kafkaConsumer.commitSync(offsetData);
                    } catch (WakeupException wakeup) {
                        log.error("Wakeup on closing commitSync, retrying");
                        kafkaConsumer.commitSync(offsetData);
                    }
                } catch (Throwable e) {
                    log.error(controllerName + " error commiting sync", e);
                }
            } catch (InterruptedException e) {
                log.info("Control-Loop failed to commit offsets on shutdown", e);
            }
            //the kafkaConsumer must be closed in the poll thread
            log.info(controllerName + "Control-Loop closing kafkaConsumer");
            kafkaConsumer.close();
        }
    }

    private Map<TopicPartition, List<KafkaMessage>> consumerRecordsToKafkaMessages(ConsumerRecords<?, ?> records) {
        Map<TopicPartition, List<KafkaMessage>> messages = new HashMap<>();
        records.partitions().forEach(tp -> messages.put(tp, new ArrayList<>()));
        for (ConsumerRecord<?, ?> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            messages.get(partition).add(new KafkaMessage(this, record));
        }
        return messages;
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info(controllerName + "Control-Loop partitions revoked");
            try {
                Map<TopicPartition, OffsetAndMetadata>
                                removedOffsetData =
                                acknowlegmentHandler.removePartitions(partitions);
                for(TopicPartition partition : partitions){
                    flowControl.remove(partition);
                }

                kafkaConsumer.commitSync(removedOffsetData);
            } catch (InterruptedException e) {
                log.info("Control-Loop partition rebalance interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info(controllerName + "Control-Loop partitions assigned");
            acknowlegmentHandler.addPartitions(partitions);
            for(TopicPartition partition : partitions){
                flowControl.merge(partition, new AtomicInteger(),
                                       (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
            }
        }

    }
}
