package com.moglix.kafka.fallback.boot2.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.exception.KafkaFallbackException;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Spring Kafka ProducerListener that handles message delivery outcomes asynchronously.
 * On success: records success on the circuit breaker and completes pending futures.
 * On error: records failure on the circuit breaker, trips to HTTP fallback service,
 * and completes pending futures or logs disaster if both Kafka and HTTP fallback fail.
 */
public class KafkaFallbackProducerListener implements ProducerListener<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(KafkaFallbackProducerListener.class);
    public static final String CORRELATION_HEADER = "x-kafka-fallback-correlation-id";

    private final CircuitBreaker circuitBreaker;
    private final FallbackHttpClient fallbackHttpClient;
    private final String applicationName;
    private final ObjectMapper objectMapper;
    private final ProducerListener<Object, Object> delegate;
    private final ConcurrentHashMap<String, PendingMessageContext> pendingContexts = new ConcurrentHashMap<>();

    public KafkaFallbackProducerListener(CircuitBreaker circuitBreaker,
                                         FallbackHttpClient fallbackHttpClient,
                                         String applicationName,
                                         ObjectMapper objectMapper) {
        this(circuitBreaker, fallbackHttpClient, applicationName, objectMapper, null);
    }

    public KafkaFallbackProducerListener(CircuitBreaker circuitBreaker,
                                         FallbackHttpClient fallbackHttpClient,
                                         String applicationName,
                                         ObjectMapper objectMapper,
                                         ProducerListener<Object, Object> delegate) {
        this.circuitBreaker = circuitBreaker;
        this.fallbackHttpClient = fallbackHttpClient;
        this.applicationName = (applicationName != null && !applicationName.trim().isEmpty())
                ? applicationName.trim() : "unknown-application";
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.delegate = delegate;
    }

    public static class PendingMessageContext {
        private final String topic;
        private final Object fallbackPayload;
        private final String rawPayloadString;
        private final Map<String, String> originalHeaders;
        private final CompletableFuture<PublishResult> future;

        public PendingMessageContext(String topic,
                                     Object fallbackPayload,
                                     String rawPayloadString,
                                     Map<String, String> originalHeaders,
                                     CompletableFuture<PublishResult> future) {
            this.topic = topic;
            this.fallbackPayload = fallbackPayload;
            this.rawPayloadString = rawPayloadString;
            this.originalHeaders = originalHeaders;
            this.future = future;
        }

        public String getTopic() {
            return topic;
        }

        public Object getFallbackPayload() {
            return fallbackPayload;
        }

        public String getRawPayloadString() {
            return rawPayloadString;
        }

        public Map<String, String> getOriginalHeaders() {
            return originalHeaders;
        }

        public CompletableFuture<PublishResult> getFuture() {
            return future;
        }
    }

    public void registerPending(String correlationId, PendingMessageContext context) {
        this.pendingContexts.put(correlationId, context);
    }

    public PendingMessageContext removePending(String correlationId) {
        return this.pendingContexts.remove(correlationId);
    }

    public int getPendingCount() {
        return this.pendingContexts.size();
    }

    public void handleTimeout(String correlationId, TimeoutException timeoutException) {
        PendingMessageContext context = removePending(correlationId);
        if (context != null && !context.getFuture().isDone()) {
            circuitBreaker.recordFailure();
            log.warn("Kafka publish timed out for topic '{}': {}. Tripping to fallback HTTP service",
                    context.getTopic(), timeoutException.getMessage());
            dispatchToFallback(
                    context.getTopic(),
                    context.getFallbackPayload(),
                    context.getRawPayloadString(),
                    context.getOriginalHeaders(),
                    timeoutException,
                    context.getFuture()
            );
        }
    }

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        circuitBreaker.recordSuccess();
        int partition = recordMetadata != null ? recordMetadata.partition() : -1;
        long offset = recordMetadata != null ? recordMetadata.offset() : -1;
        String topic = producerRecord != null ? producerRecord.topic() : "unknown";
        log.debug("Successfully published message to Kafka topic '{}', partition: {}, offset: {}",
                topic, partition, offset);

        String correlationId = extractCorrelationId(producerRecord);
        if (correlationId != null) {
            PendingMessageContext context = removePending(correlationId);
            if (context != null && !context.getFuture().isDone()) {
                context.getFuture().complete(PublishResult.sentToKafka(context.getTopic()));
            }
        }

        if (delegate != null) {
            try {
                delegate.onSuccess(producerRecord, recordMetadata);
            } catch (Throwable t) {
                log.warn("Error in delegate ProducerListener.onSuccess: {}", t.getMessage());
            }
        }
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord,
                        RecordMetadata recordMetadata,
                        Exception exception) {
        circuitBreaker.recordFailure();

        String topic = producerRecord != null ? producerRecord.topic() : "unknown";
        log.warn("Kafka publish failed for topic '{}': {}. Tripping to fallback HTTP service",
                topic, exception != null ? exception.getMessage() : "Unknown Kafka error");

        String correlationId = extractCorrelationId(producerRecord);
        PendingMessageContext context = correlationId != null ? removePending(correlationId) : null;

        if (context != null) {
            dispatchToFallback(
                    context.getTopic(),
                    context.getFallbackPayload(),
                    context.getRawPayloadString(),
                    context.getOriginalHeaders(),
                    exception,
                    context.getFuture()
            );
        } else if (producerRecord != null) {
            handleUncorrelatedError(producerRecord, exception);
        }

        if (delegate != null) {
            try {
                delegate.onError(producerRecord, recordMetadata, exception);
            } catch (Throwable t) {
                log.warn("Error in delegate ProducerListener.onError: {}", t.getMessage());
            }
        }
    }

    public void dispatchToFallback(String topic,
                                   Object fallbackPayload,
                                   String payloadString,
                                   Map<String, String> headers,
                                   Throwable kafkaError,
                                   CompletableFuture<PublishResult> resultFuture) {
        try {
            FallbackMessageRequest request = new FallbackMessageRequest(
                    topic,
                    this.applicationName,
                    fallbackPayload,
                    headers
            );

            FallbackApiResponse response = fallbackHttpClient.send(request);
            String messageId = response != null ? response.getMessageId() : null;
            if (resultFuture != null && !resultFuture.isDone()) {
                resultFuture.complete(PublishResult.buffered(topic, messageId));
            }
        } catch (Throwable fallbackError) {
            // Disaster scenario: Both Kafka and Fallback API failed!
            log.error("CRITICAL DISASTER: Both Kafka and Fallback API failed! Message could not be delivered. " +
                            "Topic: '{}', Application: '{}', Payload: '{}', Headers: '{}'. Kafka error: '{}', Fallback error: '{}'",
                    topic, this.applicationName, payloadString, headers,
                    kafkaError != null ? kafkaError.getMessage() : "Circuit Breaker OPEN",
                    fallbackError.getMessage(), fallbackError);

            KafkaFallbackException ex = new KafkaFallbackException(
                    "Both Kafka delivery and fallback buffering failed for topic: " + topic,
                    topic,
                    kafkaError,
                    fallbackError
            );
            if (resultFuture != null && !resultFuture.isDone()) {
                resultFuture.completeExceptionally(ex);
            }
        }
    }

    private void handleUncorrelatedError(ProducerRecord<Object, Object> record, Exception exception) {
        String topic = record.topic();
        Object recordValue = record.value();
        Object fallbackPayload = recordValue;
        String payloadString = String.valueOf(recordValue);

        if (recordValue instanceof String) {
            try {
                fallbackPayload = objectMapper.readTree((String) recordValue);
            } catch (Exception ignored) {
                fallbackPayload = recordValue;
            }
        }

        Map<String, String> headers = extractHeaders(record);
        dispatchToFallback(topic, fallbackPayload, payloadString, headers, exception, null);
    }

    private String extractCorrelationId(ProducerRecord<?, ?> record) {
        if (record == null || record.headers() == null) {
            return null;
        }
        Header header = record.headers().lastHeader(CORRELATION_HEADER);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    private Map<String, String> extractHeaders(ProducerRecord<?, ?> record) {
        if (record == null || record.headers() == null) {
            return null;
        }
        Map<String, String> headers = new HashMap<>();
        for (Header h : record.headers()) {
            if (h.key() != null && !CORRELATION_HEADER.equals(h.key())) {
                headers.put(h.key(), h.value() != null ? new String(h.value(), StandardCharsets.UTF_8) : "");
            }
        }
        return headers.isEmpty() ? null : headers;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public FallbackHttpClient getFallbackHttpClient() {
        return fallbackHttpClient;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
