package com.moglix.kafka.fallback.boot3.properties;

import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot 3 configuration properties binding for kafka.fallback.*
 */
@ConfigurationProperties(prefix = "kafka.fallback")
public class Boot3KafkaFallbackProperties extends KafkaFallbackProperties {
    private static final long serialVersionUID = 1L;
}
