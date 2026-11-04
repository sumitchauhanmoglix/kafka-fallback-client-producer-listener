package com.moglix.kafka.fallback.model;

import com.moglix.kafka.fallback.enums.PublishStatus;

import java.io.Serializable;
import java.util.Objects;

/**
 * Result of publishing a message through KafkaFallbackOperations.
 */
public class PublishResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PublishStatus status;
    private final String messageId;
    private final String topic;
    private final long timestamp;

    public PublishResult(PublishStatus status, String messageId, String topic) {
        this(status, messageId, topic, System.currentTimeMillis());
    }

    public PublishResult(PublishStatus status, String messageId, String topic, long timestamp) {
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.messageId = messageId;
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.timestamp = timestamp;
    }

    public static PublishResult sentToKafka(String topic) {
        return new PublishResult(PublishStatus.SENT_TO_KAFKA, null, topic);
    }

    public static PublishResult buffered(String topic, String messageId) {
        return new PublishResult(PublishStatus.BUFFERED, messageId, topic);
    }

    public PublishStatus getStatus() {
        return status;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isSentToKafka() {
        return status == PublishStatus.SENT_TO_KAFKA;
    }

    public boolean isBuffered() {
        return status == PublishStatus.BUFFERED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishResult that = (PublishResult) o;
        return timestamp == that.timestamp &&
                status == that.status &&
                Objects.equals(messageId, that.messageId) &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, messageId, topic, timestamp);
    }

    @Override
    public String toString() {
        return "PublishResult{" +
                "status=" + status +
                ", messageId='" + messageId + '\'' +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
