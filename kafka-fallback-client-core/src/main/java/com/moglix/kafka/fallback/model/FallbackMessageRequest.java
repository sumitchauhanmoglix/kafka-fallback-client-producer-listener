package com.moglix.kafka.fallback.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Fallback request contract matching JIRA 1's POST /v1/messages.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FallbackMessageRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;
    private String applicationName;
    private Object payload;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> headers;

    public FallbackMessageRequest() {
    }

    public FallbackMessageRequest(String topic, String applicationName, Object payload, Map<String, String> headers) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.applicationName = Objects.requireNonNull(applicationName, "applicationName must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.headers = (headers == null || headers.isEmpty()) ? null : Collections.unmodifiableMap(headers);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = (headers == null || headers.isEmpty()) ? null : Collections.unmodifiableMap(headers);
    }

    @Override
    public String toString() {
        return "FallbackMessageRequest{" +
                "topic='" + topic + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", payload=" + payload +
                ", headers=" + headers +
                '}';
    }
}
