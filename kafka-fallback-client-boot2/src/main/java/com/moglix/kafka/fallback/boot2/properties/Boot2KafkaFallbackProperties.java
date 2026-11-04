package com.moglix.kafka.fallback.boot2.properties;

import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot 2 configuration properties binding for kafka.fallback.*
 */
@ConfigurationProperties(prefix = "kafka.fallback")
public class Boot2KafkaFallbackProperties extends KafkaFallbackProperties {
    private static final long serialVersionUID = 1L;
}
