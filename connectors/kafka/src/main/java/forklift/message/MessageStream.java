package forklift.message;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A stream of available messages which can be retrieved by topic.
 */
public class MessageStream implements ReadableMessageStream {
    private static final Logger log = LoggerFactory.getLogger(MessageStream.class);
    Map<String, BlockingQueue<KafkaMessage>> topicQueue = new ConcurrentHashMap<>();

    /**
     * Adds the passed in records to the stream.  After being added, a record is available to be retreived through a
     * call to {@link #nextRecord(String, long)}.
     * <p>
     * <strong>Note:</strong> All passed in records must belong to a topic added through a call to {@link #addTopic(String)}.
     *
     * @param records the records to add
     * @throws IllegalStateException if the capacity of a stream has been exceeded.
     */
    public void addRecords(Map<TopicPartition, List<KafkaMessage>> records) {
        log.debug("Adding records to stream");
        for (TopicPartition partition : records.keySet()) {
            for (KafkaMessage message : records.get(partition)) {
                BlockingQueue<KafkaMessage> queue = topicQueue.get(partition.topic());
                if (queue != null) {
                    queue.add(message);
                }
            }
        }
    }

    /**
     * Configures this stream to receive messages for a given topic.
     *
     * @param topic the topic to add
     */
    public void addTopic(String topic) {
        //The capacity of the blocking queue is never expected to be hit as the Consumer should close the topic if it crashes.
        //This is mainly in place as a safeguard against a memory leak and blocking the consumption of messages
        this.topicQueue.put(topic, new LinkedBlockingQueue<>(100000));
    }

    /**
     * Removes a topic from the stream.  Any queued up messages belonging to the removed topic are discarded.
     *
     * @param topic the topic to remove
     */
    public void removeTopic(String topic) {
        this.topicQueue.remove(topic);
    }

    @Override
    public KafkaMessage nextRecord(String topic, long timeout) throws InterruptedException {
        KafkaMessage message = null;
        BlockingQueue<KafkaMessage> queue = topicQueue.get(topic);
        if (queue != null) {
            message = queue.poll(timeout, TimeUnit.MILLISECONDS);
        }
        return message;
    }
}
