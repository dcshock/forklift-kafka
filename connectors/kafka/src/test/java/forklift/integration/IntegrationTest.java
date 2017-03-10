package forklift.integration;

import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.AvroMessage;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationTest {

    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
//    private static boolean ordered = true;
//    private static boolean isPropsSet = false;
//    private static boolean isHeadersSet = false;
//    private static boolean isPropOverwritten = true;

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
        private Map<String,String> mapMessage;

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



    @BeforeClass
    public static void beforeClass() {
        TestServiceManager.start();
    }

    @Before
    public void before() {
        called.set(0);
        isInjectNull = true;
    }

    @AfterClass
    public static void afterClass() {
        TestServiceManager.stop();
    }


    @Test
    public void testAvroMessage() throws ProducerException, ConnectorException, InterruptedException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("forklift-avro-topic");
        for (int i = 0; i < msgCount; i++) {
            AvroMessage avroMessage = new AvroMessage();
            avroMessage.setName("Avro Message Name");
            producer.send(avroMessage);
        }
        final Consumer c = new Consumer(ForkliftAvroConsumer.class, TestServiceManager.getConnector());
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
    public void testStringMessage() throws ProducerException, ConnectorException, InterruptedException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }
        final Consumer c = new Consumer(StringConsumer.class, TestServiceManager.getConnector());
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
    public void testMultiThreadedStringMessage() throws ProducerException, ConnectorException, InterruptedException {
        int msgCount = 100;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }
        final Consumer c = new Consumer(MultiThreadedStringConsumer.class, TestServiceManager.getConnector());
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
    public void testSendMapValueMessage() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("forklift-map-topic");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            producer.send(m);
        }

        final Consumer c = new Consumer(ForkliftMapConsumer.class, TestServiceManager.getConnector());
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
    public void testSendObjectMessage() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("forklift-object-topic");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);
            producer.send(m);
        }

        final Consumer c = new Consumer(ForkliftObjectConsumer.class, TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() == msgCount);
    }
    


}
