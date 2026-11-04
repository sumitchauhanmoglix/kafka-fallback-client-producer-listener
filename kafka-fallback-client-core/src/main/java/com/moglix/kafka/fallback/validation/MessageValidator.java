package com.moglix.kafka.fallback.validation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.exception.InvalidMessageException;

import java.io.IOException;

/**
 * Validates message attributes before any network call.
 */
public class MessageValidator {

    private final ObjectMapper objectMapper;

    public MessageValidator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    /**
     * Validates topic and payload.
     *
     * @param topic   target topic
     * @param payload raw JSON string or serializable Java Object
     * @return ValidatedPayload containing normalized representations for Kafka and Fallback HTTP
     * @throws InvalidMessageException if validation fails
     */
    public ValidatedPayload validate(String topic, Object payload) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new InvalidMessageException("Topic must not be null or blank");
        }

        if (payload == null) {
            throw new InvalidMessageException("Payload must not be null");
        }

        String kafkaPayloadString;
        Object fallbackPayload;

        if (payload instanceof String) {
            String strPayload = ((String) payload).trim();
            if (strPayload.isEmpty()) {
                throw new InvalidMessageException("Payload string must not be empty or blank");
            }
            try (JsonParser parser = objectMapper.getFactory().createParser(strPayload)) {
                JsonNode jsonNode = objectMapper.readTree(parser);
                if (jsonNode == null || parser.nextToken() != null) {
                    throw new InvalidMessageException("Payload string contains trailing or invalid JSON content: " + strPayload);
                }
                kafkaPayloadString = strPayload;
                fallbackPayload = jsonNode;
            } catch (JsonProcessingException e) {
                throw new InvalidMessageException("Payload string is not valid JSON: " + e.getMessage(), e);
            } catch (IOException e) {
                throw new InvalidMessageException("Error parsing payload JSON: " + e.getMessage(), e);
            }
        } else {
            try {
                // Validate serializability and convert to JSON string for Kafka
                kafkaPayloadString = objectMapper.writeValueAsString(payload);
                fallbackPayload = payload;
            } catch (JsonProcessingException e) {
                throw new InvalidMessageException("Failed to serialize payload object to JSON: " + e.getMessage(), e);
            }
        }

        return new ValidatedPayload(topic.trim(), kafkaPayloadString, fallbackPayload);
    }

    public static class ValidatedPayload {
        private final String topic;
        private final String kafkaPayloadString;
        private final Object fallbackPayload;

        public ValidatedPayload(String topic, String kafkaPayloadString, Object fallbackPayload) {
            this.topic = topic;
            this.kafkaPayloadString = kafkaPayloadString;
            this.fallbackPayload = fallbackPayload;
        }

        public String getTopic() {
            return topic;
        }

        public String getKafkaPayloadString() {
            return kafkaPayloadString;
        }

        public Object getFallbackPayload() {
            return fallbackPayload;
        }
    }
}
