package forklift.connectors;

import forklift.message.Header;

import java.util.HashMap;
import java.util.Map;

public class ForkliftMessage {
    private String msg;
    private boolean flagged;
    private String warning;
    private Map<Header, Object> headers;
    private Map<String, Object> properties;

    public ForkliftMessage() {
    }

    public ForkliftMessage(String msg) {
        this.setMsg(msg);
    }

    public void acknowledge() throws ConnectorException {

    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    public boolean isFlagged() {
        return flagged;
    }

    public void setFlagged(boolean flagged) {
        this.flagged = flagged;
    }

    public void setHeaders(Map<Header, Object> headers) {
        // Get rid of unmodifiable.
        final Map<Header, Object> newHeaders = new HashMap<>();
        headers.keySet().stream().forEach(key -> newHeaders.put(key, headers.get(key)));
        this.headers = newHeaders;
    }

    public Map<Header, Object> getHeaders() {
        return headers;
    }

    public void setProperties(Map<String, Object> properties) {
        // Get rid of unmodifiable.
        final Map<String, Object> newProps = new HashMap<>();
        properties.keySet().stream().forEach(key -> newProps.put(key, properties.get(key)));
        this.properties = newProps;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
