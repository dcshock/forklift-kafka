package forklift.consumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.KafkaController;
import forklift.connectors.MessageStream;

/**
 * Retrieves messages from a kafka topic and adds consumer's topic to the {@link forklift.connectors.KafkaController}.
 */
public class KafkaTopicConsumer implements ForkliftConsumerI {
    private final String topic;
    private final KafkaController controller;
    private final MessageStream messageStream;
    private volatile boolean topicAdded = false;

    public KafkaTopicConsumer(String topic, KafkaController controller, MessageStream messageStream) {
        this.topic = topic;
        this.controller = controller;
        this.messageStream = messageStream;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Retrieves the {@link forklift.connectors.MessageStream#nextRecord(String, long) nextRecord} from the messageStream.
     * If no record is available within the specified timeout, null is returned.
     * <p>
     * <strong>Note:</strong> Because we do not wish to poll for kafka topics until we are actively receiving messages, this method is
     * also responsible for calling {@link forklift.connectors.KafkaController#addTopic(String) addTopic} on the kafkaController.
     *
     * @param timeout the time in milliseconds to wait for a record to become available
     * @return a message if one is available, else null
     * @throws ConnectorException if the process is interrupted or the controller is no longer running
     */
    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        try {
            if (!topicAdded) {
                synchronized (this) {
                    if (!topicAdded) {
                        controller.addTopic(topic);
                        topicAdded = true;
                    }
                }

            }
            //ensure that our controller is still running
            if (!controller.isRunning()) {
                throw new ConnectorException("Connection to Kafka Controller lost");
            }
            return messageStream.nextRecord(this.topic, timeout);
        } catch (InterruptedException e) {
            throw new ConnectorException("Kafka message receive interrupted");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Removes this consumer's topic from the controller.  Future calls to {@link #receive(long) receive} will re-add the
     * topic to the controller.
     * </p>
     */
    @Override
    public void close() {
        controller.removeTopic(topic);
    }
}
