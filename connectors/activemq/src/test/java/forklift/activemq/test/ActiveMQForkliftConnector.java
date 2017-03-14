package forklift.activemq.test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;
import javax.inject.Named;

/**
 * Wrap the activemq connector with a spring annotation so that forklift can
 * resolve the provider.
 *
 * @author mconroy
 */
@Named
public class ActiveMQForkliftConnector implements ForkliftConnectorI {

    @Override
    public void start() throws ConnectorException {
        TestServiceManager.getConnector().start();
    }

    @Override
    public void stop() throws ConnectorException {
        TestServiceManager.getConnector().stop();
    }

    @Override
    public ForkliftConsumerI getQueue(String name) throws ConnectorException {
        return TestServiceManager.getConnector().getQueue(name);
    }

    @Override
    public ForkliftConsumerI getTopic(String name) throws ConnectorException {
        return TestServiceManager.getConnector().getTopic(name);
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        return TestServiceManager.getConnector().getQueueProducer(name);
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
        return TestServiceManager.getConnector().getTopicProducer(name);
    }
}
