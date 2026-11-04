package com.moglix.kafka.fallback.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.exception.KafkaFallbackException;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.http.RestTemplateFallbackHttpClient;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import com.moglix.kafka.fallback.resilience.SimpleCircuitBreaker;
import com.moglix.kafka.fallback.validation.MessageValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base implementation of KafkaFallbackOperations.
 * Coordinates validation, circuit-breaking, Kafka dispatch, and HTTP fallback failover.
 */
public abstract class AbstractKafkaFallbackClient implements KafkaFallbackOperations {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaFallbackClient.class);

    private final KafkaFallbackProperties properties;
    private final MessageValidator validator;
    private final CircuitBreaker circuitBreaker;
    private final FallbackHttpClient fallbackHttpClient;
    private final String applicationName;
    private final ScheduledExecutorService scheduler;

    public AbstractKafkaFallbackClient(KafkaFallbackProperties properties,
                                       String defaultApplicationName,
                                       ObjectMapper objectMapper,
                                       CircuitBreaker circuitBreaker,
                                       FallbackHttpClient fallbackHttpClient) {
        this.properties = properties != null ? properties : new KafkaFallbackProperties();
        this.validator = new MessageValidator(objectMapper);
        
        // Resolve application name: config property > constructor default (spring.application.name) > "unknown-application"
        if (this.properties.getApplicationName() != null && !this.properties.getApplicationName().trim().isEmpty()) {
            this.applicationName = this.properties.getApplicationName().trim();
        } else if (defaultApplicationName != null && !defaultApplicationName.trim().isEmpty()) {
            this.applicationName = defaultApplicationName.trim();
        } else {
            this.applicationName = "unknown-application";
        }

        this.circuitBreaker = circuitBreaker != null ? circuitBreaker :
                new SimpleCircuitBreaker(
                        this.properties.getCircuitBreaker().getFailureThreshold(),
                        this.properties.getCircuitBreaker().getWaitDurationInOpenMs()
                );

        this.fallbackHttpClient = fallbackHttpClient != null ? fallbackHttpClient :
                new RestTemplateFallbackHttpClient(
                        this.properties.getApiUrl(),
                        this.properties.getApiKey(),
                        objectMapper,
                        this.properties.getTimeouts().getApiConnectTimeoutMs(),
                        this.properties.getTimeouts().getApiReadTimeoutMs(),
                        this.properties.getRetry().getMaxAttempts(),
                        this.properties.getRetry().getBackoffMs()
                );

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kafka-fallback-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public PublishResult publish(String topic, String jsonPayload) {
        return publish(topic, (Object) jsonPayload, null);
    }

    @Override
    public PublishResult publish(String topic, String jsonPayload, Map<String, String> headers) {
        return publish(topic, (Object) jsonPayload, headers);
    }

    @Override
    public PublishResult publish(String topic, Object payload) {
        return publish(topic, payload, null);
    }

    @Override
    public PublishResult publish(String topic, Object payload, Map<String, String> headers) {
        try {
            long totalTimeout = properties.getTimeouts().getKafkaSendTimeoutMs() +
                    properties.getTimeouts().getApiConnectTimeoutMs() +
                    properties.getTimeouts().getApiReadTimeoutMs();
            return publishAsync(topic, payload, headers).get(totalTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new KafkaFallbackException("Failed to publish message: " + cause.getMessage(), topic, cause, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaFallbackException("Publish interrupted for topic: " + topic, topic, e, null);
        } catch (TimeoutException e) {
            throw new KafkaFallbackException("Publish call timed out for topic: " + topic, topic, e, null);
        }
    }

    @Override
    public CompletableFuture<PublishResult> publishAsync(String topic, Object payload) {
        return publishAsync(topic, payload, null);
    }

    @Override
    public CompletableFuture<PublishResult> publishAsync(String topic, Object payload, Map<String, String> headers) {
        // 1. Fail-fast validation before any network call
        MessageValidator.ValidatedPayload validated = validator.validate(topic, payload);

        CompletableFuture<PublishResult> resultFuture = new CompletableFuture<>();

        // 2. Check circuit breaker
        if (!circuitBreaker.allowRequest()) {
            log.warn("Circuit breaker is OPEN. Bypassing Kafka and routing directly to fallback HTTP service for topic '{}'", validated.getTopic());
            dispatchToFallback(validated, headers, null, resultFuture);
            return resultFuture;
        }

        // 3. Dispatch to Kafka; outcome handled by ProducerListener
        doSendToKafka(
                validated.getTopic(),
                validated.getKafkaPayloadString(),
                validated.getFallbackPayload(),
                headers,
                resultFuture
        );

        return resultFuture;
    }

    public void dispatchToFallback(String topic,
                                   Object fallbackPayload,
                                   String kafkaPayloadString,
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
                    topic, this.applicationName, kafkaPayloadString, headers,
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

    public void dispatchToFallback(MessageValidator.ValidatedPayload validated,
                                   Map<String, String> headers,
                                   Throwable kafkaError,
                                   CompletableFuture<PublishResult> resultFuture) {
        dispatchToFallback(
                validated.getTopic(),
                validated.getFallbackPayload(),
                validated.getKafkaPayloadString(),
                headers,
                kafkaError,
                resultFuture
        );
    }

    private <T> void attachTimeout(CompletableFuture<T> future, long timeoutMs) {
        if (timeoutMs <= 0) return;
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.completeExceptionally(new TimeoutException("Kafka send timed out after " + timeoutMs + " ms"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Module-specific Kafka dispatch with result future for ProducerListener outcome handling.
     * Subclasses using ProducerListener override this method.
     *
     * @param topic           target topic
     * @param payload         JSON string payload
     * @param fallbackPayload structured payload (Object or JsonNode)
     * @param headers         optional headers
     * @param resultFuture    future to be completed by ProducerListener outcome
     */
    protected void doSendToKafka(String topic,
                                 String payload,
                                 Object fallbackPayload,
                                 Map<String, String> headers,
                                 CompletableFuture<PublishResult> resultFuture) {
        CompletableFuture<Void> kafkaSendFuture = doSendToKafka(topic, payload, fallbackPayload, headers);
        long kafkaTimeoutMs = properties.getTimeouts().getKafkaSendTimeoutMs();

        attachTimeout(kafkaSendFuture, kafkaTimeoutMs);

        kafkaSendFuture.whenComplete((ignored, kafkaError) -> {
            if (kafkaError == null) {
                circuitBreaker.recordSuccess();
                log.debug("Published directly to Kafka topic '{}'", topic);
                resultFuture.complete(PublishResult.sentToKafka(topic));
            } else {
                circuitBreaker.recordFailure();
                log.warn("Kafka publish failed or timed out for topic '{}': {}. Tripping to fallback HTTP service",
                        topic, kafkaError.getMessage());
                dispatchToFallback(topic, fallbackPayload, payload, headers, kafkaError, resultFuture);
            }
        });
    }

    /**
     * Module-specific Kafka dispatch with fallbackPayload available for serializer adaptation.
     *
     * @param topic           target topic
     * @param payload         JSON string payload
     * @param fallbackPayload structured payload (Object or JsonNode)
     * @param headers         optional headers
     * @return future that completes when Kafka acks the message
     */
    protected CompletableFuture<Void> doSendToKafka(String topic, String payload, Object fallbackPayload, Map<String, String> headers) {
        return doSendToKafka(topic, payload, headers);
    }

    protected CompletableFuture<Void> doSendToKafka(String topic, String payload, Map<String, String> headers) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        f.complete(null);
        return f;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public MessageValidator getValidator() {
        return validator;
    }

    public KafkaFallbackProperties getProperties() {
        return properties;
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
}
