package com.moglix.kafka.fallback.boot3.autoconfigure;

import com.moglix.kafka.fallback.boot3.listener.KafkaFallbackProducerListener;
import com.moglix.kafka.fallback.client.KafkaFallbackOperations;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaFallbackBoot3AutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaFallbackBoot3AutoConfiguration.class))
            .withBean(RestTemplate.class, () -> mock(RestTemplate.class))
            .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class));

    @Test
    @DisplayName("Auto-configuration should register KafkaFallbackOperations and KafkaFallbackProducerListener when enabled")
    void testAutoConfigurationRegistersBean() {
        contextRunner
                .withPropertyValues("spring.application.name=my-test-service", "kafka.fallback.api-url=http://fallback:8080/v1/messages")
                .run(context -> {
                    assertThat(context).hasSingleBean(KafkaFallbackOperations.class);
                    assertThat(context).hasSingleBean(KafkaFallbackProducerListener.class);
                    assertThat(context).hasSingleBean(ProducerListener.class);
                    assertThat(context).hasSingleBean(KafkaFallbackProperties.class);
                    KafkaFallbackProperties props = context.getBean(KafkaFallbackProperties.class);
                    assertThat(props.getApiUrl()).isEqualTo("http://fallback:8080/v1/messages");
                });
    }

    @Test
    @DisplayName("Auto-configuration should back off when kafka.fallback.enabled is false")
    void testAutoConfigurationDisabled() {
        contextRunner
                .withPropertyValues("kafka.fallback.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(KafkaFallbackOperations.class);
                    assertThat(context).doesNotHaveBean(KafkaFallbackProducerListener.class);
                });
    }
}
