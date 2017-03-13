package forklift.retry;

import forklift.consumer.ProcessStep;

import java.util.Map;

public class RetryMessage {
    private String messageId;
    private String queue;
    private String topic;
    private ProcessStep step;
    private String text;
    private Map<String, String> properties;
    private String persistedPath;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public ProcessStep getStep() {
        return step;
    }

    public void setStep(ProcessStep step) {
        this.step = step;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getPersistedPath() {
        return persistedPath;
    }

    public void setPersistedPath(String persistedPath) {
        this.persistedPath = persistedPath;
    }

    @Override
    public String toString() {
        return "RetryMessage [messageId=" + messageId + ", queue=" + queue + ", topic=" + topic + ", step=" + step + ", text=" + text +
                        ", properties=" + properties + ", persistedPath=" + persistedPath + "]";
    }
}
