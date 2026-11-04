package com.moglix.kafka.fallback.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.exception.InvalidMessageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageValidatorTest {

    private MessageValidator validator;

    @BeforeEach
    void setUp() {
        validator = new MessageValidator(new ObjectMapper());
    }

    @Test
    @DisplayName("Should accept valid topic and valid JSON string")
    void testValidJsonString() {
        String json = "{\"orderId\":\"123\",\"status\":\"CREATED\"}";
        MessageValidator.ValidatedPayload result = validator.validate("orders", json);

        assertEquals("orders", result.getTopic());
        assertEquals(json, result.getKafkaPayloadString());
        assertNotNull(result.getFallbackPayload());
    }

    @Test
    @DisplayName("Should accept valid Java POJO / Map and serialize to JSON")
    void testValidPojoPayload() {
        Map<String, Object> map = Collections.singletonMap("key", "val");
        MessageValidator.ValidatedPayload result = validator.validate("orders", map);

        assertEquals("orders", result.getTopic());
        assertEquals("{\"key\":\"val\"}", result.getKafkaPayloadString());
        assertEquals(map, result.getFallbackPayload());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t\n"})
    @DisplayName("Should reject null, empty or blank topic")
    void testInvalidTopic(String invalidTopic) {
        assertThrows(InvalidMessageException.class, () ->
                validator.validate(invalidTopic, "{\"orderId\":\"123\"}"));
    }

    @Test
    @DisplayName("Should reject null payload")
    void testNullPayload() {
        assertThrows(InvalidMessageException.class, () ->
                validator.validate("topic", null));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"   ", "\t\n"})
    @DisplayName("Should reject empty or blank payload string")
    void testBlankPayloadString(String blankPayload) {
        assertThrows(InvalidMessageException.class, () ->
                validator.validate("topic", blankPayload));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not a json", "{unclosed json", "123 invalid", "foo: bar"})
    @DisplayName("Should reject invalid JSON strings before any network call")
    void testInvalidJsonString(String invalidJson) {
        assertThrows(InvalidMessageException.class, () ->
                validator.validate("topic", invalidJson));
    }
}
