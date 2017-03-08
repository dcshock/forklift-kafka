package forklift.integration;

import static org.junit.Assert.assertTrue;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Queue("q2")
public class ProducerTest {
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean ordered = true;
    private static boolean isInjectNull = true;
    private static boolean isPropsSet = false;
    private static boolean isHeadersSet = false;
    private static boolean isPropOverwritten = true;

    @forklift.decorators.Message
    private ForkliftMessage m;

    @Producer(queue = "q2")
    private ForkliftProducerI injectedProducer;

    @Before
    public void before() {
        TestServiceManager.start();
        called.set(0);
        isInjectNull = true;
    }

    @After
    public void after() {
        TestServiceManager.stop();
    }

    @OnMessage
    public void onMessage() {
        if (m == null)
            return;

        int i = called.getAndIncrement();
        System.out.println(Thread.currentThread().getName() + m);
        isInjectNull = injectedProducer != null ? false : true;
    }

    @Test
    public void testSendStringMessage() throws ProducerException, ConnectorException, InterruptedException {

        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");

            producer.send(msg);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());

        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Test
    public void testSendObjectMessage() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);
            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Test
    public void testSendKeyValueMessage() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Test
    public void testSendTripleThreat() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final ForkliftMessage m = new ForkliftMessage();
            m.setMsg("x=producer overload test");

            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(ordered);
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
            assertTrue("injectedProducer is null", isInjectNull == false);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    public class TestMessage {
        private String text;
        private int someNumber;

        public TestMessage(String text, int someNumber) {
            this.text = text;
            this.someNumber = someNumber;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public int getSomeNumber() {
            return someNumber;
        }

        public void setSomeNumber(int someNumber) {
            this.someNumber = someNumber;
        }
    }
}
