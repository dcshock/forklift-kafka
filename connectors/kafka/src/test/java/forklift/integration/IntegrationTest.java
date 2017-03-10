package forklift.integration;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.AvroMessage;
import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

public class IntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;
//    private static boolean ordered = true;
//    private static boolean isPropsSet = false;
//    private static boolean isHeadersSet = false;
//    private static boolean isPropOverwritten = true;


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

    @Test
    public void testAvroMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-avro-topic");
        for (int i = 0; i < msgCount; i++) {
            AvroMessage avroMessage = new AvroMessage();
            avroMessage.setName("Avro Message Name");
            producer.send(avroMessage);
        }
        final Consumer c = new Consumer(ForkliftAvroConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Test
    public void testStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }
        final Consumer c = new Consumer(StringConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Test
    public void testMultiThreadedStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 100;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }
        final Consumer
                        c =
                        new Consumer(MultiThreadedStringConsumer.class,
                                     connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Test
    public void testSendMapValueMessage() throws ConnectorException, ProducerException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-map-topic");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            producer.send(m);
        }

        final Consumer c = new Consumer(ForkliftMapConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() == msgCount);
    }

    @Test
    public void testSendObjectMessage() throws ConnectorException, ProducerException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-object-topic");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);
            producer.send(m);
        }

        final Consumer c = new Consumer(ForkliftObjectConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() == msgCount);
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

        ExecutorService executor = Executors.newFixedThreadPool(12);

        //3 Forklift instances
        Forklift forklift1 = serviceManager.newManagedForkliftInstance();
        Forklift forklift2 = serviceManager.newManagedForkliftInstance();
        Forklift forklift3 = serviceManager.newManagedForkliftInstance();

        //3 Connector instances
        ForkliftConnectorI connector1 = forklift1.getConnector();
        ForkliftConnectorI connector2 = forklift2.getConnector();
        ForkliftConnectorI connector3 = forklift3.getConnector();

        //3 producers, these all come from one connector as we do not want to take producers down.
        ForkliftProducerI producer1 = connector3.getQueueProducer("forklift-string-topic");
        ForkliftProducerI producer2 = connector3.getQueueProducer("forklift-map-topic");
        ForkliftProducerI producer3 = connector3.getQueueProducer("forklift-object-topic");

        List<Consumer>
                        connector1Consumers =
                        setupConsumers(connector1, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        List<Consumer>
                        connector2Consumers =
                        setupConsumers(connector2, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        List<Consumer>
                        connector3Consumers =
                        setupConsumers(connector3, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);

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
        Thread.sleep(2500);
        connector2Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));
        Thread.sleep(2500);
        connector3Consumers.forEach(consumer -> executor.submit(() -> consumer.listen()));

        Thread.sleep(5000);
        log.info("STOPPING CONSUMER 1");
        connector1Consumers.forEach(consumer -> consumer.shutdown());
        forklift1.shutdown();
        Thread.sleep(5000);
        //stop a consumer
        log.info("STOPPING CONSUMER 2");
        connector2Consumers.forEach(consumer -> consumer.shutdown());
        forklift2.shutdown();
        Thread.sleep(5000);
        //stop producing
        running.set(false);
        //wait to finish any processing
        Thread.sleep(5000);
        //stop another consumer
        log.info("STOPPING CONSUMER 3");
        connector3Consumers.forEach(consumer -> consumer.shutdown());
        forklift3.shutdown();
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

    @Queue("forklift-message-topic")
    public static class ForkliftMessageConsumer {

        @forklift.decorators.Message
        private ForkliftMessage message;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (message == null || message.getMsg() == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + message.getMsg());
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
