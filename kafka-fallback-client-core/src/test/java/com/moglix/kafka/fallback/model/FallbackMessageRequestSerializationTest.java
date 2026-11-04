package com.moglix.kafka.fallback.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FallbackMessageRequestSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("When headers are null, the headers field must be completely absent in JSON")
    void testHeadersOmittedWhenNull() throws Exception {
        Map<String, Object> payload = Collections.singletonMap("orderId", "ORD-1001");
        FallbackMessageRequest request = new FallbackMessageRequest("order-events", "order-service", payload, null);

        String json = objectMapper.writeValueAsString(request);
        JsonNode node = objectMapper.readTree(json);

        assertEquals("order-events", node.get("topic").asText());
        assertEquals("order-service", node.get("applicationName").asText());
        assertEquals("ORD-1001", node.get("payload").get("orderId").asText());
        assertFalse(node.has("headers"), "headers field must be absent when null");
    }

    @Test
    @DisplayName("When headers map is empty, the headers field must be completely absent in JSON")
    void testHeadersOmittedWhenEmpty() throws Exception {
        Map<String, Object> payload = Collections.singletonMap("orderId", "ORD-1001");
        FallbackMessageRequest request = new FallbackMessageRequest("order-events", "order-service", payload, Collections.emptyMap());

        String json = objectMapper.writeValueAsString(request);
        JsonNode node = objectMapper.readTree(json);

        assertFalse(node.has("headers"), "headers field must be absent when empty");
    }

    @Test
    @DisplayName("When headers are provided, headers field must be present in JSON with key-value pairs")
    void testHeadersPresentWhenProvided() throws Exception {
        Map<String, Object> payload = Collections.singletonMap("orderId", "ORD-1001");
        Map<String, String> headers = new HashMap<>();
        headers.put("traceId", "abc-123");
        headers.put("spanId", "def-456");

        FallbackMessageRequest request = new FallbackMessageRequest("order-events", "order-service", payload, headers);

        String json = objectMapper.writeValueAsString(request);
        JsonNode node = objectMapper.readTree(json);

        assertTrue(node.has("headers"), "headers field must be present when populated");
        assertEquals("abc-123", node.get("headers").get("traceId").asText());
        assertEquals("def-456", node.get("headers").get("spanId").asText());
    }
}
