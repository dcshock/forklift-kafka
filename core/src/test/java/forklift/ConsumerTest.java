package forklift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.Message;
import forklift.decorators.Properties;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class ConsumerTest {
    @Test(expected = IllegalArgumentException.class)
    public void createBadConsumer() {
        new Consumer(BadConsumer.class, null, this.getClass().getClassLoader());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDoubleConsumer() {
        new Consumer(DoubleConsumer.class, null, this.getClass().getClassLoader());
    }

    @Test
    public void createQueueConsumer() {
        Consumer c = new Consumer(QueueConsumer.class, null, this.getClass().getClassLoader());
        assertTrue(c.getName().matches("abc:\\d"));
    }

    @Test
    public void createTopicConsumer() {
        Consumer c = new Consumer(TopicConsumer.class, null, this.getClass().getClassLoader());
        assertTrue(c.getName().matches("xyz:\\d"));
    }

    @Test
    public void inject() throws ConnectorException {
        Consumer test = new Consumer(ExampleConsumer.class, null, this.getClass().getClassLoader());
        ExampleConsumer ec = new ExampleConsumer();
        //ForkliftMessage jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("x=y\nname=Scooby Doo\n");

        test.inject(msg, ec);

        // Now assert ec properties and make sure they are correct.
        assertEquals(2, ec.kv.size());
        assertEquals("y", ec.kv.get("x"));
        assertEquals("Scooby Doo", ec.kv.get("name"));
        assertEquals("x=y\nname=Scooby Doo\n", ec.fmsg.getMsg());
        // assertEquals("1", ec.fmsg.getJmsMsg().getJMSCorrelationID());
        assertEquals("x=y\nname=Scooby Doo\n", ec.str);
    }

    // TODO put this back in with a real test.
    // The system shouldn't hand the bad json to the consumer, and let the consumer mark the message as invalid to avoid redelivery of a bad message.
    // @Test(expected=RuntimeException.class)
    public void injectBadJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, null, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        // javax.jms.Message jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("x=y");

        test.inject(msg, ec);
    }

    @Test
    public void injectEmptyJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, null, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        // javax.jms.Message jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("{}");

        test.inject(msg, ec);
        assertNotNull(ec.msg);
        assertNull(ec.msg.ideas);
        assertNull(ec.msg.name);
        assertNull(ec.msg.url);
    }

    @Test
    public void injectJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, null, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        //javax.jms.Message jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("{\"name\":\"Fred Jones\", \"url\":\"http://forklift\", \"ideas\":[\"scanning\", \"verifying\"]}");

        test.inject(msg, ec);
        assertNotNull(ec.msg);
        assertTrue("scanning".equals(ec.msg.ideas[0]));
        assertEquals(2, ec.msg.ideas.length);
        assertEquals("Fred Jones", ec.msg.name);
        assertEquals("http://forklift", ec.msg.url);
        assertEquals(0, ec.properties.size());
        assertEquals("default", ec.strval);
    }

    @Test
    public void testHeadersAndProperties() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, null, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        // javax.jms.Message jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("{}");

        Map<String, String> properties = new HashMap<>();
        properties.put("my-cool-prop", "3");
        properties.put("mystrval", "blah");
        properties.put("my-long-val", "123123");
        properties.put("my-float-val", "123123");
        msg.setProperties(properties);

        test.inject(msg, ec);
        assertEquals(4, ec.properties.size());
        assertEquals("blah", ec.mystrval);
        assertEquals("blah", ec.strval);
    }

    // Class doesn't have queue or topic should throw IllegalArgException
    public class BadConsumer {
    }

    @Queue("q")
    @Topic("a")
    public class DoubleConsumer {

    }

    @Queue("abc")
    public class QueueConsumer {

    }

    @Topic("xyz")
    public class TopicConsumer {

    }

    @Queue("abc")
    public class ExampleConsumer {
        @Message
        ForkliftMessage fmsg;

        @Message
        Map<String, String> kv;

        @Message
        String str;
    }

    @Queue("a")
    public class ExampleJsonConsumer {
        ;

        @Properties
        Map<String, Object> properties;

        @Properties("mystrval")
        String strval = "default";

        @Properties
        String mystrval;

        @Message
        ForkliftMessage fmsg;

        @Message
        Map<String, String> kv;

        @Message
        String str;

        @Message
        ExpectedMsg msg;
    }

    public static class ExpectedMsg {
        public String name;
        public String url;
        public String[] ideas;

        public ExpectedMsg() {
        }
    }

}
