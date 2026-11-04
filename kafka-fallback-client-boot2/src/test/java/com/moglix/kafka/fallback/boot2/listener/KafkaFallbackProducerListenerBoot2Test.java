package com.moglix.kafka.fallback.boot2.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.enums.PublishStatus;
import com.moglix.kafka.fallback.exception.KafkaFallbackException;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.resilience.SimpleCircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.ProducerListener;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaFallbackProducerListenerBoot2Test {

    private SimpleCircuitBreaker circuitBreaker;
    private FallbackHttpClient fallbackHttpClient;
    private ObjectMapper objectMapper;
    private ProducerListener<Object, Object> mockDelegate;
    private KafkaFallbackProducerListener listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        circuitBreaker = new SimpleCircuitBreaker(2, 5000);
        fallbackHttpClient = mock(FallbackHttpClient.class);
        objectMapper = new ObjectMapper();
        mockDelegate = mock(ProducerListener.class);

        listener = new KafkaFallbackProducerListener(
                circuitBreaker,
                fallbackHttpClient,
                "boot2-listener-app",
                objectMapper,
                mockDelegate
        );
    }

    @Test
    @DisplayName("onSuccess should record success, complete future, and invoke delegate")
    void testOnSuccessWithCorrelation() throws Exception {
        String correlationId = "corr-101";
        CompletableFuture<PublishResult> future = new CompletableFuture<>();
        KafkaFallbackProducerListener.PendingMessageContext context =
                new KafkaFallbackProducerListener.PendingMessageContext(
                        "orders",
                        "{\"id\":1}",
                        "{\"id\":1}",
                        Collections.emptyMap(),
                        future
                );

        listener.registerPending(correlationId, context);
        assertEquals(1, listener.getPendingCount());

        ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "{\"id\":1}");
        record.headers().add(new RecordHeader(KafkaFallbackProducerListener.CORRELATION_HEADER,
                correlationId.getBytes(StandardCharsets.UTF_8)));
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders", 1), 0, 10, 0L, 0, 0);

        listener.onSuccess(record, metadata);

        assertTrue(future.isDone());
        PublishResult result = future.get();
        assertEquals(PublishStatus.SENT_TO_KAFKA, result.getStatus());
        assertEquals("orders", result.getTopic());
        assertEquals(0, listener.getPendingCount());

        verify(mockDelegate).onSuccess(record, metadata);
        verify(fallbackHttpClient, never()).send(any());
    }

    @Test
    @DisplayName("onError with correlation should trip to HTTP fallback and complete future with BUFFERED")
    void testOnErrorWithCorrelation() throws Exception {
        String correlationId = "corr-202";
        CompletableFuture<PublishResult> future = new CompletableFuture<>();
        Map<String, String> headers = Collections.singletonMap("traceId", "trace-xyz");
        KafkaFallbackProducerListener.PendingMessageContext context =
                new KafkaFallbackProducerListener.PendingMessageContext(
                        "orders",
                        "{\"id\":2}",
                        "{\"id\":2}",
                        headers,
                        future
                );

        listener.registerPending(correlationId, context);

        when(fallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenReturn(new FallbackApiResponse("msg-buffered-202", "BUFFERED"));

        ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "{\"id\":2}");
        record.headers().add(new RecordHeader(KafkaFallbackProducerListener.CORRELATION_HEADER,
                correlationId.getBytes(StandardCharsets.UTF_8)));

        Exception kafkaException = new RuntimeException("Broker unreachable");
        listener.onError(record, null, kafkaException);

        assertTrue(future.isDone());
        PublishResult result = future.get();
        assertEquals(PublishStatus.BUFFERED, result.getStatus());
        assertEquals("msg-buffered-202", result.getMessageId());
        assertEquals(0, listener.getPendingCount());

        ArgumentCaptor<FallbackMessageRequest> captor = ArgumentCaptor.forClass(FallbackMessageRequest.class);
        verify(fallbackHttpClient).send(captor.capture());
        FallbackMessageRequest sentRequest = captor.getValue();
        assertEquals("orders", sentRequest.getTopic());
        assertEquals("boot2-listener-app", sentRequest.getApplicationName());
        assertEquals(headers, sentRequest.getHeaders());

        verify(mockDelegate).onError(record, null, kafkaException);
    }

    @Test
    @DisplayName("onError for direct send without correlation header should route to HTTP fallback")
    void testOnErrorDirectSendWithoutCorrelation() throws Exception {
        when(fallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenReturn(new FallbackApiResponse("msg-direct-303", "BUFFERED"));

        ProducerRecord<Object, Object> record = new ProducerRecord<>("notifications", "{\"alert\":\"cpu_high\"}");
        record.headers().add(new RecordHeader("source", "monitor".getBytes(StandardCharsets.UTF_8)));

        listener.onError(record, null, new RuntimeException("Topic not found"));

        ArgumentCaptor<FallbackMessageRequest> captor = ArgumentCaptor.forClass(FallbackMessageRequest.class);
        verify(fallbackHttpClient).send(captor.capture());
        FallbackMessageRequest sentRequest = captor.getValue();
        assertEquals("notifications", sentRequest.getTopic());
        assertEquals("boot2-listener-app", sentRequest.getApplicationName());
        assertNotNull(sentRequest.getHeaders());
        assertEquals("monitor", sentRequest.getHeaders().get("source"));
    }

    @Test
    @DisplayName("onError when both Kafka and Fallback API fail should complete exceptionally")
    void testOnErrorDisasterFailsExceptionally() throws Exception {
        String correlationId = "corr-disaster";
        CompletableFuture<PublishResult> future = new CompletableFuture<>();
        KafkaFallbackProducerListener.PendingMessageContext context =
                new KafkaFallbackProducerListener.PendingMessageContext(
                        "critical-events",
                        "{\"alert\":\"system_down\"}",
                        "{\"alert\":\"system_down\"}",
                        null,
                        future
                );

        listener.registerPending(correlationId, context);

        when(fallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenThrow(new RuntimeException("HTTP 503 Service Unavailable"));

        ProducerRecord<Object, Object> record = new ProducerRecord<>("critical-events", "{\"alert\":\"system_down\"}");
        record.headers().add(new RecordHeader(KafkaFallbackProducerListener.CORRELATION_HEADER,
                correlationId.getBytes(StandardCharsets.UTF_8)));

        listener.onError(record, null, new RuntimeException("Kafka unreachable"));

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof KafkaFallbackException);
    }

    @Test
    @DisplayName("handleTimeout should remove pending, trip circuit breaker, and dispatch to fallback")
    void testHandleTimeout() throws Exception {
        String correlationId = "corr-timeout";
        CompletableFuture<PublishResult> future = new CompletableFuture<>();
        KafkaFallbackProducerListener.PendingMessageContext context =
                new KafkaFallbackProducerListener.PendingMessageContext(
                        "payments",
                        "{\"amount\":100}",
                        "{\"amount\":100}",
                        null,
                        future
                );

        listener.registerPending(correlationId, context);

        when(fallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenReturn(new FallbackApiResponse("msg-timeout-505", "BUFFERED"));

        listener.handleTimeout(correlationId, new TimeoutException("Kafka timeout 2000ms"));

        assertTrue(future.isDone());
        PublishResult result = future.get();
        assertEquals(PublishStatus.BUFFERED, result.getStatus());
        assertEquals("msg-timeout-505", result.getMessageId());
        assertEquals(0, listener.getPendingCount());
    }
}
