package forklift.replay;

import forklift.consumer.ProcessStep;

import java.util.List;
import java.util.Map;

public class ReplayMsg {
    String messageId;
    String queue;
    String topic;
    ProcessStep step;
    String text;
    Map<String, String> properties;
    List<String> errors;
    String time;

    public String getMessageId() {
        return messageId;
    }

    public String getQueue() {
        return queue;
    }

    public String getTopic() {
        return topic;
    }

    public ProcessStep getStep() {
        return step;
    }

    public String getText() {
        return text;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<String> getErrors() {
        return errors;
    }

    public String getTime() {
        return time;
    }
}
