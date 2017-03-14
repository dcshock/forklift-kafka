package forklift.producers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import forklift.connectors.ForkliftMessage;
import java.util.Map;
import java.util.UUID;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class ActiveMQProducer implements ForkliftProducerI {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private MessageProducer producer;
    private Destination destination;
    private Map<String, String> properties;
    private Session session;

    public ActiveMQProducer(MessageProducer producer, Session session) {
        this.producer = producer;
        this.session = session;
    }

    @Override
    /**
     * Send in a string and the producer generate a ForkliftMessage with it
     * @param message - String representation of the message/data that needs to be sent
     * @return String - JMSCorrelationID
     **/
    public String send(String message) throws ProducerException {
        try {
            return send(new ForkliftMessage(message));
        } catch (Exception e) {
            throw new ProducerException("Failed to send message", e);
        }
    }

    @Override
    /**
     * Send in an object and the producer will marshall it into a ForkliftMessage
     * @param message - Object representation of the message/data that needs to be sent
     * @return String - JMSCorrelationID
     **/
    public String send(Object message) throws ProducerException {
        try {
            return send(new ForkliftMessage(mapper.writeValueAsString(message)));
        } catch (Exception e) {
            throw new ProducerException("Failed to send message", e);
        }
    }

    @Override
    /**
     * Send in a map and the producer will convert it to a key-value pair before sending a ForkliftMessage
     * @param message - Map<String, String> of the message/data that needs to be sent
     * @return String - JMSCorrelationID
     **/
    public String send(Map<String, String> message) throws ProducerException {
        final StringBuilder output = new StringBuilder();
        message.forEach((k, v) -> output.append(k).append('=').append(v).append('\n'));
        return send(new ForkliftMessage(output.toString()));
    }

    @Override
    /**
     * Send a ForkliftMessage.
     * @param message - ForkliftMessage
     * @return String - JMSCorrelationID
     **/
    public String send(ForkliftMessage message) throws ProducerException {
        try {
            Message msg = prepAndValidate(new forklift.message.ActiveMQMessage(message));
            if (msg != null) {
                producer.send(msg);
                return msg.getJMSCorrelationID();
            } else {
                throw new ProducerException("Forklift MSG is null");
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to send message", e);
        }
    }

    @Override
    /**
     * Send in an object and the producer will marshall it into a ForkliftMessage
     * @param properties - message properties
     * @param message - ForkliftMessage
     * @return String - JMSCorrelationID
     **/
    public String send(Map<String, String> properties,
                       ForkliftMessage message) throws ProducerException {
        Message msg = prepAndValidate(new forklift.message.ActiveMQMessage(message), properties);
        try {
            producer.send(msg);
            return msg.getJMSCorrelationID();
        } catch (Exception e) {
            throw new ProducerException("Failed to send message", e);
        }
    }

    /**
     * validate message and prepare for sending.
     *
     * @param message - ForkliftMessage to be checked
     * @return - jms message ready to be sent to endpoint
     **/
    private Message prepAndValidate(forklift.message.ActiveMQMessage message) throws ProducerException {
        Message msg = forkliftToJms(message);
        // check the ID
        try {
            if (Strings.isNullOrEmpty(msg.getJMSCorrelationID())) {
                msg.setJMSCorrelationID(UUID.randomUUID().toString().replaceAll("-", ""));
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to set setJMSCorrelationID");
        }

        try {
            if (this.properties != null) {
                setMessageProperties(msg, this.properties);
            }
        } catch (Exception e) {
            throw new ProducerException("Exception while setting message properties/headers with producer properties/headers", e);
        }
        return msg;
    }

    /**
     * validate message and prepare for sending.
     *
     * @param message    - ForkliftMessage to be checked
     * @param properties - message properties to be set
     * @return - jms message ready to be sent to endpoint
     **/
    private Message prepAndValidate(forklift.message.ActiveMQMessage message,
                                    Map<String, String> properties) throws ProducerException {
        Message msg = forkliftToJms(message);
        try {
            if (Strings.isNullOrEmpty(msg.getJMSCorrelationID())) {
                msg.setJMSCorrelationID(UUID.randomUUID().toString().replaceAll("-", ""));
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to set setJMSCorrelationID");
        }
        try {
            setMessageProperties(msg, properties);
        } catch (Exception e) {
            throw new ProducerException("Failed to set message property", e);
        }

        return msg;
    }

    /**
     * Generate javax.jms.Message from a Forklift Message
     *
     * @param message - Forklift message to be converted
     * @return - a new javax.jms.Message
     **/
    private Message forkliftToJms(forklift.message.ActiveMQMessage message) throws ProducerException {

        try {
            Message msg = null;
            if (message.getMsg() != null) {
                try {
                    msg = session.createTextMessage(message.getMsg());
                } catch (Exception e) {
                    throw new ProducerException("Error creating new jms TextMessage", e);
                }
            }
            setMessageProperties(msg, message.getProperties());
            return msg;
        } catch (Exception e) {
            throw new ProducerException("Error creating JMS Message, Forklift message was null", e);
        }
    }

    private void setMessageProperties(Message msg, Map<String, String> properties) throws ProducerException {
        if (msg != null && properties != null) {
            properties.entrySet().stream().filter(entry -> entry.getValue() != null)
                      .forEach(property -> {
                          try {
                              if (msg.getObjectProperty(property.getKey()) == null) {
                                  msg.setObjectProperty(property.getKey(), property.getValue());
                              }
                          } catch (Exception e) {
                              //Catch it but just move on.
                          }
                      });
        }
    }

    public int getDeliveryMode() throws ProducerException {
        try {
            return this.producer.getDeliveryMode();
        } catch (Exception e) {
            throw new ProducerException("Failed to get DelieveryMode");
        }
    }

    public void setDeliveryMode(int mode) throws ProducerException {
        try {
            this.producer.setDeliveryMode(mode);
        } catch (Exception e) {
            throw new ProducerException("Failed to set DelieveryMode");
        }
    }

    public long getTimeToLive() throws ProducerException {
        try {
            return this.producer.getTimeToLive();
        } catch (Exception e) {
            throw new ProducerException("Failed to get TimeToLive");
        }
    }

    public void setTimeToLive(long timeToLive) throws ProducerException {
        try {
            this.producer.setTimeToLive(timeToLive);
        } catch (Exception e) {
            throw new ProducerException("Failed to set TimeToLive");
        }
    }

    @Override
    public Map<String, String> getProperties() throws ProducerException {
        try {
            return this.properties;
        } catch (Exception e) {
            throw new ProducerException("Failed to get properties");
        }
    }

    @Override
    public void setProperties(Map<String, String> properties) throws ProducerException {
        try {
            this.properties = properties;
        } catch (Exception e) {
            throw new ProducerException("Failed to set properties");
        }
    }

    @Override
    public void close() throws java.io.IOException {
        try {
            if (producer != null)
                producer.close();

            if (session != null)
                session.close();
        } catch (javax.jms.JMSException e) {
            throw new java.io.IOException("Failed to close MessageProducer", e);
        }
    }
}
