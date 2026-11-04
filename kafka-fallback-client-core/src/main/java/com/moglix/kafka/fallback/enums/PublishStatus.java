package com.moglix.kafka.fallback.enums;

/**
 * Status of the message publication.
 */
public enum PublishStatus {
    /**
     * Message was successfully published directly to Kafka.
     */
    SENT_TO_KAFKA,

    /**
     * Kafka failed/timed out, and the message was buffered via the HTTP fallback service.
     */
    BUFFERED
}
