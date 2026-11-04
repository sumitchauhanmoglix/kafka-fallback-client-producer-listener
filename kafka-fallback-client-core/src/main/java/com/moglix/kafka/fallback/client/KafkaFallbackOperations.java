package com.moglix.kafka.fallback.client;

import com.moglix.kafka.fallback.model.PublishResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Public util API for resilient Kafka publishing with HTTP fallback.
 * Java 8 compatible signatures.
 */
public interface KafkaFallbackOperations {

    /**
     * Publishes a raw JSON string to Kafka with fallback buffer on failure.
     *
     * @param topic       target Kafka topic
     * @param jsonPayload raw JSON string
     * @return PublishResult (SENT_TO_KAFKA or BUFFERED)
     */
    PublishResult publish(String topic, String jsonPayload);

    /**
     * Publishes a raw JSON string to Kafka with custom headers and fallback buffer on failure.
     *
     * @param topic       target Kafka topic
     * @param jsonPayload raw JSON string
     * @param headers     optional headers map
     * @return PublishResult (SENT_TO_KAFKA or BUFFERED)
     */
    PublishResult publish(String topic, String jsonPayload, Map<String, String> headers);

    /**
     * Publishes a Java Object (serialized to JSON) with fallback buffer on failure.
     *
     * @param topic   target Kafka topic
     * @param payload object to serialize
     * @return PublishResult (SENT_TO_KAFKA or BUFFERED)
     */
    PublishResult publish(String topic, Object payload);

    /**
     * Publishes a Java Object with custom headers and fallback buffer on failure.
     *
     * @param topic   target Kafka topic
     * @param payload object to serialize
     * @param headers optional headers map
     * @return PublishResult (SENT_TO_KAFKA or BUFFERED)
     */
    PublishResult publish(String topic, Object payload, Map<String, String> headers);

    /**
     * Asynchronously publishes a message with fallback buffer on failure.
     *
     * @param topic   target Kafka topic
     * @param payload object or raw JSON string
     * @return CompletableFuture of PublishResult
     */
    CompletableFuture<PublishResult> publishAsync(String topic, Object payload);

    /**
     * Asynchronously publishes a message with custom headers and fallback buffer on failure.
     *
     * @param topic   target Kafka topic
     * @param payload object or raw JSON string
     * @param headers optional headers map
     * @return CompletableFuture of PublishResult
     */
    CompletableFuture<PublishResult> publishAsync(String topic, Object payload, Map<String, String> headers);
}
