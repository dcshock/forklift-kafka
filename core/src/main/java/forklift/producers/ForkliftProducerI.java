package forklift.producers;

import forklift.connectors.ForkliftMessage;
import java.io.Closeable;
import java.util.Map;

public interface ForkliftProducerI extends Closeable {
    String send(String message) throws ProducerException;

    String send(ForkliftMessage message) throws ProducerException;

    String send(Object message) throws ProducerException;

    String send(Map<String, String> message) throws ProducerException;

    String send(Map<String, String> properties,
                ForkliftMessage message) throws ProducerException;

    Map<String, String> getProperties() throws ProducerException;

    void setProperties(Map<String, String> properties) throws ProducerException;
}  
