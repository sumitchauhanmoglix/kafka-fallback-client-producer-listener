package com.moglix.kafka.fallback.boot3.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.boot3.client.DefaultKafkaFallbackClient;
import com.moglix.kafka.fallback.boot3.listener.KafkaFallbackProducerListener;
import com.moglix.kafka.fallback.boot3.properties.Boot3KafkaFallbackProperties;
import com.moglix.kafka.fallback.client.KafkaFallbackOperations;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.http.RestTemplateFallbackHttpClient;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import com.moglix.kafka.fallback.resilience.SimpleCircuitBreaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestTemplate;

/**
 * Spring Boot 3 AutoConfiguration for Kafka Fallback Client using ProducerListener.
 */
@AutoConfiguration
@EnableConfigurationProperties(Boot3KafkaFallbackProperties.class)
@ConditionalOnClass({KafkaTemplate.class, RestTemplate.class})
@ConditionalOnProperty(prefix = "kafka.fallback", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaFallbackBoot3AutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(CircuitBreaker.class)
    public CircuitBreaker kafkaFallbackCircuitBreaker(Boot3KafkaFallbackProperties properties) {
        return new SimpleCircuitBreaker(
                properties.getCircuitBreaker().getFailureThreshold(),
                properties.getCircuitBreaker().getWaitDurationInOpenMs()
        );
    }

    @Bean
    @ConditionalOnMissingBean(FallbackHttpClient.class)
    public FallbackHttpClient kafkaFallbackHttpClient(
            Boot3KafkaFallbackProperties properties,
            @Autowired(required = false) ObjectMapper objectMapper) {
        return new RestTemplateFallbackHttpClient(
                properties.getApiUrl(),
                properties.getApiKey(),
                objectMapper,
                properties.getTimeouts().getApiConnectTimeoutMs(),
                properties.getTimeouts().getApiReadTimeoutMs(),
                properties.getRetry().getMaxAttempts(),
                properties.getRetry().getBackoffMs()
        );
    }

    @Bean
    @ConditionalOnMissingBean(KafkaFallbackProducerListener.class)
    public KafkaFallbackProducerListener kafkaFallbackProducerListener(
            Boot3KafkaFallbackProperties properties,
            Environment environment,
            CircuitBreaker circuitBreaker,
            FallbackHttpClient fallbackHttpClient,
            @Autowired(required = false) ObjectMapper objectMapper) {

        String appName = properties.getApplicationName();
        if (appName == null || appName.trim().isEmpty()) {
            appName = environment.getProperty("spring.application.name", "unknown-application");
        }

        return new KafkaFallbackProducerListener(
                circuitBreaker,
                fallbackHttpClient,
                appName,
                objectMapper != null ? objectMapper : new ObjectMapper()
        );
    }

    @Bean
    @ConditionalOnMissingBean(KafkaFallbackOperations.class)
    public KafkaFallbackOperations kafkaFallbackOperations(
            Boot3KafkaFallbackProperties properties,
            Environment environment,
            @Autowired(required = false) ObjectMapper objectMapper,
            CircuitBreaker circuitBreaker,
            FallbackHttpClient fallbackHttpClient,
            KafkaFallbackProducerListener producerListener,
            @Autowired(required = false) KafkaTemplate<?, ?> kafkaTemplate,
            @Autowired(required = false) ApplicationContext applicationContext) {

        String appName = properties.getApplicationName();
        if (appName == null || appName.trim().isEmpty()) {
            appName = environment.getProperty("spring.application.name", "unknown-application");
        }

        KafkaTemplate<?, ?> chosenTemplate = kafkaTemplate;
        if (applicationContext != null && applicationContext.containsBean("stringKafkaTemplate")) {
            try {
                chosenTemplate = (KafkaTemplate<?, ?>) applicationContext.getBean("stringKafkaTemplate");
            } catch (Throwable ignored) {
            }
        }

        return new DefaultKafkaFallbackClient(
                properties,
                appName,
                objectMapper != null ? objectMapper : new ObjectMapper(),
                circuitBreaker,
                fallbackHttpClient,
                chosenTemplate,
                producerListener
        );
    }
}
