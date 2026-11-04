package com.moglix.kafka.fallback.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestTemplateFallbackHttpClientTest {

    @Test
    @DisplayName("Should configure RestTemplate with the provided custom ObjectMapper")
    void testCustomObjectMapperConfiguredInRestTemplate() {
        ObjectMapper customMapper = new ObjectMapper();
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient(
                "http://localhost:8080/v1/messages",
                "secret-key",
                customMapper,
                1000,
                2000,
                2,
                100
        );

        boolean found = false;
        for (HttpMessageConverter<?> converter : client.getRestTemplate().getMessageConverters()) {
            if (converter instanceof MappingJackson2HttpMessageConverter) {
                MappingJackson2HttpMessageConverter jacksonConverter = (MappingJackson2HttpMessageConverter) converter;
                assertSame(customMapper, jacksonConverter.getObjectMapper(),
                        "RestTemplate's Jackson converter must use the custom ObjectMapper passed from parent app");
                found = true;
                break;
            }
        }

        assertTrue(found, "MappingJackson2HttpMessageConverter must be present in RestTemplate");
    }
}
