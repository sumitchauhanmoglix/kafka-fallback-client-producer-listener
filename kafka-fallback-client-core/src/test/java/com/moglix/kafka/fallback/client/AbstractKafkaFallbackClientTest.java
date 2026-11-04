package com.moglix.kafka.fallback.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.exception.InvalidMessageException;
import com.moglix.kafka.fallback.exception.KafkaFallbackException;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.enums.PublishStatus;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.SimpleCircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractKafkaFallbackClientTest {

    private FallbackHttpClient mockFallbackHttpClient;
    private SimpleCircuitBreaker circuitBreaker;
    private KafkaFallbackProperties properties;
    private TestClient testClient;

    private static class TestClient extends AbstractKafkaFallbackClient {
        boolean failKafka = false;
        String capturedTopic;
        String capturedPayload;
        Map<String, String> capturedHeaders;

        TestClient(KafkaFallbackProperties properties,
                   SimpleCircuitBreaker circuitBreaker,
                   FallbackHttpClient fallbackHttpClient) {
            super(properties, "test-app", new ObjectMapper(), circuitBreaker, fallbackHttpClient);
        }

        @Override
        protected CompletableFuture<Void> doSendToKafka(String topic, String payload, Map<String, String> headers) {
            this.capturedTopic = topic;
            this.capturedPayload = payload;
            this.capturedHeaders = headers;

            CompletableFuture<Void> future = new CompletableFuture<>();
            if (failKafka) {
                future.completeExceptionally(new RuntimeException("Kafka broker down"));
            } else {
                future.complete(null);
            }
            return future;
        }
    }

    @BeforeEach
    void setUp() {
        mockFallbackHttpClient = mock(FallbackHttpClient.class);
        circuitBreaker = new SimpleCircuitBreaker(2, 5000);
        properties = new KafkaFallbackProperties();
        properties.getTimeouts().setKafkaSendTimeoutMs(500);

        testClient = new TestClient(properties, circuitBreaker, mockFallbackHttpClient);
    }

    @Test
    @DisplayName("Healthy Kafka sends directly and does not call fallback HTTP service")
    void testHealthyKafkaSend() throws Exception {
        Map<String, String> headers = Collections.singletonMap("traceId", "123");
        PublishResult result = testClient.publish("orders", "{\"id\":1}", headers);

        assertEquals(PublishStatus.SENT_TO_KAFKA, result.getStatus());
        assertEquals("orders", result.getTopic());
        assertNull(result.getMessageId());
        assertTrue(result.isSentToKafka());
        assertFalse(result.isBuffered());

        assertEquals("orders", testClient.capturedTopic);
        assertEquals("{\"id\":1}", testClient.capturedPayload);
        assertEquals(headers, testClient.capturedHeaders);

        verify(mockFallbackHttpClient, never()).send(any());
    }

    @Test
    @DisplayName("Kafka failure routes to fallback HTTP service and returns BUFFERED with messageId")
    void testKafkaFailureRoutesToFallback() throws Exception {
        testClient.failKafka = true;
        when(mockFallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenReturn(new FallbackApiResponse("msg-999", "BUFFERED"));

        PublishResult result = testClient.publish("orders", "{\"id\":1}");

        assertEquals(PublishStatus.BUFFERED, result.getStatus());
        assertEquals("orders", result.getTopic());
        assertEquals("msg-999", result.getMessageId());
        assertTrue(result.isBuffered());
        assertFalse(result.isSentToKafka());

        ArgumentCaptor<FallbackMessageRequest> captor = ArgumentCaptor.forClass(FallbackMessageRequest.class);
        verify(mockFallbackHttpClient).send(captor.capture());

        FallbackMessageRequest req = captor.getValue();
        assertEquals("orders", req.getTopic());
        assertEquals("test-app", req.getApplicationName());
        assertNotNull(req.getPayload());
        assertNull(req.getHeaders());
    }

    @Test
    @DisplayName("When both Kafka and Fallback API fail, throws KafkaFallbackException")
    void testBothFailThrowsException() throws Exception {
        testClient.failKafka = true;
        when(mockFallbackHttpClient.send(any(FallbackMessageRequest.class)))
                .thenThrow(new RuntimeException("Fallback HTTP 500 Internal Server Error"));

        KafkaFallbackException ex = assertThrows(KafkaFallbackException.class, () ->
                testClient.publish("orders", "{\"id\":1}"));

        assertEquals("orders", ex.getTopic());
        assertTrue(ex.getMessage().contains("orders"));
    }

    @Test
    @DisplayName("Fail-fast validation throws InvalidMessageException before any network attempts")
    void testValidationFailsFast() throws Exception {
        assertThrows(InvalidMessageException.class, () ->
                testClient.publish("   ", "{\"id\":1}"));

        assertThrows(InvalidMessageException.class, () ->
                testClient.publish("orders", "not-a-json"));

        verify(mockFallbackHttpClient, never()).send(any());
    }

    private static void assertNotNull(Object obj) {
        org.junit.jupiter.api.Assertions.assertNotNull(obj);
    }
}
