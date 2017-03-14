package forklift.message;

import forklift.connectors.ForkliftMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Created by afrieze on 3/14/17.
 */
public class ActiveMQMessage extends ForkliftMessage {

    private Message message;

    public ActiveMQMessage(Message message) {
        this.message = message;
        this.parseJmsMessage();
    }

    public ActiveMQMessage(String message) {
        this.msg = message;
    }

    public ActiveMQMessage(ForkliftMessage message) {
        this.msg = message.getMsg();
        if (message instanceof ActiveMQMessage) {
            this.message = ((ActiveMQMessage)message).getJmsMessage();
        }
        this.properties = message.getProperties();
    }

    public Message getJmsMessage() {
        return this.message;
    }

    @Override
    public boolean acknowledge(){
        try {
            this.message.acknowledge();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public String getId() {
        if (message != null) {
            try {
                return message.getJMSCorrelationID();
            } catch (JMSException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * <strong>WARNING:</strong> Called from constructor
     */
    private final void parseJmsMessage() {
        try {
            if (message instanceof ActiveMQTextMessage) {
                this.msg = ((ActiveMQTextMessage)message).getText();
            } else {
                this.flagged = true;
                this.warning = "Unexpected JMS message type";
            }

            // Build properties
            try {
                org.apache.activemq.command.ActiveMQMessage amq = (org.apache.activemq.command.ActiveMQMessage)message;
                Map<String, String> properties = new HashMap<>();
                if (amq.getProperties() != null) {
                    amq.getProperties().forEach((key, value) -> {
                        properties.put(key, value == null ? null : String.valueOf(value));
                    });
                }

                this.setProperties(properties);
            } catch (IOException ignored) {
                // Shouldn't happen
            }

        } catch (JMSException e) {
            this.flagged = true;
            this.warning = "JMSException in parseJmsMessage: " + e.getErrorCode() + " - " + e.getMessage();
        }
    }
}
