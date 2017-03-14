package forklift.consumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.message.ActiveMQMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class ActiveMQMessageConsumer implements ForkliftConsumerI {
    private MessageConsumer consumer;
    private Session s;

    public ActiveMQMessageConsumer(MessageConsumer consumer, Session s) {
        this.consumer = consumer;
        this.s = s;
    }

    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        try {
            Message message = consumer.receive(timeout);
            return message == null ? null : new ActiveMQMessage(message);
        } catch (JMSException e) {
            throw new ConnectorException("Error receiving ActiveMQMessage: " + e.getMessage());
        }
    }

    @Override
    public void close() throws ConnectorException {
        if (consumer != null)

            try {
                consumer.close();
            } catch (JMSException e) {
                throw new ConnectorException("Error closing MessageConsumer: " + e.getMessage());
            }

        if (s != null)
            try {
                s.close();
            } catch (JMSException e) {
                throw new ConnectorException("Error closing Session: " + e.getMessage());
            }
    }
}
