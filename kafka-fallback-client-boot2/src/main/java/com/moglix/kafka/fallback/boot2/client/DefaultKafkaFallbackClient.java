package com.moglix.kafka.fallback.boot2.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.boot2.listener.KafkaFallbackProducerListener;
import com.moglix.kafka.fallback.client.AbstractKafkaFallbackClient;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spring Boot 2 implementation of KafkaFallbackOperations using ProducerListener for message handling.
 */
public class DefaultKafkaFallbackClient extends AbstractKafkaFallbackClient {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaFallbackClient.class);

    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final KafkaFallbackProducerListener producerListener;
    private final boolean isJsonSerializer;

    public DefaultKafkaFallbackClient(KafkaFallbackProperties properties,
                                      String defaultApplicationName,
                                      ObjectMapper objectMapper,
                                      CircuitBreaker circuitBreaker,
                                      FallbackHttpClient fallbackHttpClient,
                                      KafkaTemplate<?, ?> kafkaTemplate,
                                      KafkaFallbackProducerListener producerListener) {
        super(properties, defaultApplicationName, objectMapper, circuitBreaker, fallbackHttpClient);
        this.kafkaTemplate = kafkaTemplate;
        this.producerListener = producerListener != null ? producerListener :
                new KafkaFallbackProducerListener(
                        this.getCircuitBreaker(),
                        this.getFallbackHttpClient(),
                        this.getApplicationName(),
                        objectMapper
                );
        if (this.kafkaTemplate != null) {
            attachProducerListener(this.kafkaTemplate, this.producerListener);
        }
        this.isJsonSerializer = determineIfJsonSerializer(kafkaTemplate);
    }

    public DefaultKafkaFallbackClient(KafkaFallbackProperties properties,
                                      String defaultApplicationName,
                                      ObjectMapper objectMapper,
                                      CircuitBreaker circuitBreaker,
                                      FallbackHttpClient fallbackHttpClient,
                                      KafkaTemplate<?, ?> kafkaTemplate) {
        this(properties, defaultApplicationName, objectMapper, circuitBreaker, fallbackHttpClient, kafkaTemplate, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void attachProducerListener(KafkaTemplate<?, ?> template, KafkaFallbackProducerListener listener) {
        try {
            ((KafkaTemplate) template).setProducerListener(listener);
        } catch (Throwable t) {
            log.warn("Failed to set ProducerListener on KafkaTemplate: {}", t.getMessage());
        }
    }

    private static boolean determineIfJsonSerializer(KafkaTemplate<?, ?> template) {
        if (template == null || template.getProducerFactory() == null) {
            return false;
        }
        try {
            if (template.getProducerFactory() instanceof DefaultKafkaProducerFactory) {
                Serializer<?> serializer = ((DefaultKafkaProducerFactory<?, ?>) template.getProducerFactory()).getValueSerializer();
                if (serializer != null && serializer.getClass().getName().contains("JsonSerializer")) {
                    return true;
                }
            }
            Map<String, Object> props = template.getProducerFactory().getConfigurationProperties();
            if (props != null) {
                Object serializer = props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                if (serializer != null) {
                    String className = serializer instanceof Class ? ((Class<?>) serializer).getName() : serializer.toString();
                    if (className.contains("JsonSerializer")) {
                        return true;
                    }
                }
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void doSendToKafka(String topic,
                                 String payload,
                                 Object fallbackPayload,
                                 Map<String, String> headers,
                                 CompletableFuture<PublishResult> resultFuture) {
        if (kafkaTemplate == null) {
            log.warn("KafkaTemplate is not configured in Spring application context. Tripping to fallback HTTP service");
            getCircuitBreaker().recordFailure();
            producerListener.dispatchToFallback(
                    topic,
                    fallbackPayload,
                    payload,
                    headers,
                    new IllegalStateException("KafkaTemplate is not configured in Spring application context"),
                    resultFuture
            );
            return;
        }

        String correlationId = UUID.randomUUID().toString();
        KafkaFallbackProducerListener.PendingMessageContext context =
                new KafkaFallbackProducerListener.PendingMessageContext(
                        topic,
                        fallbackPayload,
                        payload,
                        headers,
                        resultFuture
                );

        producerListener.registerPending(correlationId, context);

        long kafkaTimeoutMs = getProperties().getTimeouts().getKafkaSendTimeoutMs();
        if (kafkaTimeoutMs > 0) {
            getScheduler().schedule(() -> {
                producerListener.handleTimeout(
                        correlationId,
                        new TimeoutException("Kafka send timed out after " + kafkaTimeoutMs + " ms")
                );
            }, kafkaTimeoutMs, TimeUnit.MILLISECONDS);
        }

        ProducerRecord record = null;
        try {
            Object recordValue = (isJsonSerializer && fallbackPayload != null) ? fallbackPayload : payload;
            record = new ProducerRecord(topic, recordValue);
            record.headers().add(new RecordHeader(KafkaFallbackProducerListener.CORRELATION_HEADER,
                    correlationId.getBytes(StandardCharsets.UTF_8)));

            if (headers != null && !headers.isEmpty()) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    if (entry.getKey() != null) {
                        byte[] headerValue = entry.getValue() != null ?
                                entry.getValue().getBytes(StandardCharsets.UTF_8) : new byte[0];
                        record.headers().add(new RecordHeader(entry.getKey(), headerValue));
                    }
                }
            }

            // Dispatch to KafkaTemplate. Handled asynchronously by ProducerListener!
            ((KafkaTemplate) kafkaTemplate).send(record);
        } catch (Throwable t) {
            producerListener.onError(record, null, t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public KafkaTemplate<?, ?> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public KafkaFallbackProducerListener getProducerListener() {
        return producerListener;
    }
}
