package forklift.integration;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.AvroMessage;
import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RebalanceTest {

    private static final Logger log = LoggerFactory.getLogger(RebalanceTest.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;


    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        called.set(0);
        isInjectNull = true;
    }


    /**
     * Tests that all messages are processes when new consumers are brought up and then brought down.  Consumers are taken down in
     * the same order they are brought up in order to ensure rebalance occurs.
     * @throws StartupException
     * @throws InterruptedException
     * @throws ConnectorException
     */
    @Test
    public void testRebalancing() throws StartupException, InterruptedException, ConnectorException {

        ExecutorService executor = Executors.newFixedThreadPool(18);

        //3 Forklift instances
        Forklift forklift1 = serviceManager.newManagedForkliftInstance();
        Forklift forklift2 = serviceManager.newManagedForkliftInstance();
        Forklift forklift3 = serviceManager.newManagedForkliftInstance();
        Forklift forklift4 = serviceManager.newManagedForkliftInstance();
        Forklift forklift5 = serviceManager.newManagedForkliftInstance();

        //3 Connector instances
        ForkliftConnectorI connector1 = forklift1.getConnector();
        ForkliftConnectorI connector2 = forklift2.getConnector();
        ForkliftConnectorI connector3 = forklift3.getConnector();
        ForkliftConnectorI connector4 = forklift4.getConnector();
        ForkliftConnectorI connector5 = forklift5.getConnector();

        //3 producers, these all come from one connector as we do not want to take producers down.
        ForkliftProducerI producer1 = connector5.getQueueProducer("forklift-string-topic");
        ForkliftProducerI producer2 = connector5.getQueueProducer("forklift-map-topic");
        ForkliftProducerI producer3 = connector5.getQueueProducer("forklift-object-topic");

        List<Consumer>
                        connector1Consumers =
                        setupConsumers(connector1, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        List<Consumer>
                        connector2Consumers =
                        setupConsumers(connector2, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        List<Consumer>
                        connector3Consumers =
                        setupConsumers(connector3, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        List<Consumer>
                        connector4Consumers =
                        setupConsumers(connector4, StringConsumer.class);
        List<Consumer>
                        connector5Consumers =
                        setupConsumers(connector5, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        AtomicBoolean running = new AtomicBoolean(true);
        Random random = new Random();
        AtomicInteger messagesSent = new AtomicInteger(0);
        executor.execute(() -> {
            while (running.get()) {
                long jitter = random.nextLong() % 50;
                try {
                    producer1.send("String message");
                    messagesSent.incrementAndGet();
                    Thread.currentThread().sleep(jitter);
                } catch (Exception e) {
                }
            }
        });
        executor.execute(() -> {
            while (running.get()) {
                long jitter = random.nextLong() % 50;
                try {
                    final Map<String, String> m = new HashMap<>();
                    m.put("x", "producer key value send test");
                    producer2.send(m);
                    messagesSent.incrementAndGet();
                    Thread.currentThread().sleep(jitter);
                } catch (Exception e) {
                }
            }
        });
        executor.execute(() -> {
            while (running.get()) {
                long jitter = random.nextLong() % 50;
                try {
                    final TestMessage m = new TestMessage(new String("x=producer object send test"), 1);
                    producer3.send(m);
                    messagesSent.incrementAndGet();
                    Thread.currentThread().sleep(jitter);
                } catch (Exception e) {
                }
            }
        });
        Thread.sleep(500);
        //Start consumers
        connector1Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        connector2Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        connector3Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        Thread.sleep(1000);
        connector4Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        Thread.sleep(3000);
        connector5Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        log.info("STOPPING CONSUMER 1");
        connector1Consumers.forEach(consumer -> consumer.shutdown());
        forklift1.shutdown();
        Thread.sleep(5000);
        //stop a consumer
        log.info("STOPPING CONSUMER 2");
        connector2Consumers.forEach(consumer -> consumer.shutdown());
        forklift2.shutdown();
        log.info("STOPPING CONSUMER 3");
        connector3Consumers.forEach(consumer -> consumer.shutdown());
        forklift3.shutdown();
        Thread.sleep(5000);
        log.info("STOPPING CONSUMER 4");
        connector4Consumers.forEach(consumer -> consumer.shutdown());
        forklift4.shutdown();
        Thread.sleep(5000);
        //stop producing
        running.set(false);
        //wait to finish any processing
        Thread.sleep(5000);
        //stop another consumer
        log.info("STOPPING CONSUMER 5");
        connector5Consumers.forEach(consumer -> consumer.shutdown());
        forklift5.shutdown();
        assertEquals(messagesSent.get(), called.get());
        assertTrue(messagesSent.get() > 0);

    }

    private List<Consumer> setupConsumers(ForkliftConnectorI connector, Class<?>... consumersClasses) {
        List<Consumer> consumers = new ArrayList<>();
        for (Class<?> c : consumersClasses) {
            Consumer consumer = new Consumer(c, connector);
            consumers.add(consumer);
        }
        return consumers;
    }

    @Queue("forklift-string-topic")
    public static class StringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @Queue("forklift-avro-topic")
    public static class ForkliftAvroConsumer {

        @forklift.decorators.Message
        private AvroMessage value;

        @Producer(queue = "forklift-avro-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value.getName());
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @MultiThreaded(10)
    @Queue("forklift-string-topic")
    public static class MultiThreadedStringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @Queue("forklift-map-topic")
    public static class ForkliftMapConsumer {

        @forklift.decorators.Message
        private Map<String, String> mapMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (mapMessage == null || mapMessage.size() == 0) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + mapMessage);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }


    @Queue("forklift-object-topic")
    public static class ForkliftObjectConsumer {

        @forklift.decorators.Message
        private TestMessage testMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (testMessage == null || testMessage.getText() == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + testMessage.getText());
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

}
