package com.moglix.kafka.fallback.exception;

/**
 * Thrown when both Kafka delivery and the Fallback HTTP service fail.
 */
public class KafkaFallbackException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final Throwable kafkaCause;
    private final Throwable fallbackCause;

    public KafkaFallbackException(String message, String topic, Throwable kafkaCause, Throwable fallbackCause) {
        super(message, fallbackCause != null ? fallbackCause : kafkaCause);
        this.topic = topic;
        this.kafkaCause = kafkaCause;
        this.fallbackCause = fallbackCause;
    }

    public String getTopic() {
        return topic;
    }

    public Throwable getKafkaCause() {
        return kafkaCause;
    }

    public Throwable getFallbackCause() {
        return fallbackCause;
    }
}
