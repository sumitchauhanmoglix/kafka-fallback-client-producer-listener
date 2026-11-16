package com.moglix.kafka.fallback.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    @DisplayName("Should throw IllegalStateException when apiUrl is null, empty or blank")
    void testMissingApiUrlThrowsException() {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        RestTemplateFallbackHttpClient clientNull = new RestTemplateFallbackHttpClient(null, "key", "token", mockRestTemplate, 1, 50);
        FallbackMessageRequest request = new FallbackMessageRequest("test-topic", "test-app", "{}", null);

        assertThrows(IllegalStateException.class, () -> clientNull.send(request));
    }

    @Test
    @DisplayName("Should send X-API-Key header when apiKey is configured")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSendWithApiKeyHeader() throws Exception {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        String apiUrl = "http://fallback-service/v1/messages";
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient(apiUrl, "secret-key-123", null, mockRestTemplate, 1, 50);

        FallbackMessageRequest request = new FallbackMessageRequest("orders", "order-app", "{}", null);

        when(mockRestTemplate.exchange(eq(apiUrl), eq(HttpMethod.POST), any(HttpEntity.class), eq(FallbackApiResponse.class)))
                .thenReturn(new ResponseEntity<>(new FallbackApiResponse("m1", "BUFFERED"), HttpStatus.OK));

        client.send(request);

        ArgumentCaptor<HttpEntity> captor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(mockRestTemplate).exchange(eq(apiUrl), eq(HttpMethod.POST), captor.capture(), eq(FallbackApiResponse.class));

        HttpEntity capturedEntity = captor.getValue();
        assertEquals("secret-key-123", capturedEntity.getHeaders().getFirst("X-API-Key"));
        assertNull(capturedEntity.getHeaders().getFirst("token"));
        assertNull(capturedEntity.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
    }

    @Test
    @DisplayName("Should send Authorization (Bearer) and token headers when token is configured")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSendWithTokenHeader() throws Exception {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        String apiUrl = "http://fallback-service/v1/messages";
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient(apiUrl, null, "auth-token-xyz", mockRestTemplate, 1, 50);

        FallbackMessageRequest request = new FallbackMessageRequest("orders", "order-app", "{}", null);

        when(mockRestTemplate.exchange(eq(apiUrl), eq(HttpMethod.POST), any(HttpEntity.class), eq(FallbackApiResponse.class)))
                .thenReturn(new ResponseEntity<>(new FallbackApiResponse("m1", "BUFFERED"), HttpStatus.OK));

        client.send(request);

        ArgumentCaptor<HttpEntity> captor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(mockRestTemplate).exchange(eq(apiUrl), eq(HttpMethod.POST), captor.capture(), eq(FallbackApiResponse.class));

        HttpEntity capturedEntity = captor.getValue();
        assertEquals("auth-token-xyz", capturedEntity.getHeaders().getFirst("token"));
        assertEquals("Bearer auth-token-xyz", capturedEntity.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        assertNull(capturedEntity.getHeaders().getFirst("X-API-Key"));
    }

    @Test
    @DisplayName("Should preserve Bearer prefix if token already includes Bearer")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSendWithBearerTokenHeader() throws Exception {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        String apiUrl = "http://fallback-service/v1/messages";
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient(apiUrl, null, "Bearer custom-jwt-token", mockRestTemplate, 1, 50);

        FallbackMessageRequest request = new FallbackMessageRequest("orders", "order-app", "{}", null);

        when(mockRestTemplate.exchange(eq(apiUrl), eq(HttpMethod.POST), any(HttpEntity.class), eq(FallbackApiResponse.class)))
                .thenReturn(new ResponseEntity<>(new FallbackApiResponse("m1", "BUFFERED"), HttpStatus.OK));

        client.send(request);

        ArgumentCaptor<HttpEntity> captor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(mockRestTemplate).exchange(eq(apiUrl), eq(HttpMethod.POST), captor.capture(), eq(FallbackApiResponse.class));

        HttpEntity capturedEntity = captor.getValue();
        assertEquals("Bearer custom-jwt-token", capturedEntity.getHeaders().getFirst("token"));
        assertEquals("Bearer custom-jwt-token", capturedEntity.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
    }

    @Test
    @DisplayName("Should send both X-API-Key and token headers when both are configured")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSendWithBothApiKeyAndToken() throws Exception {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        String apiUrl = "http://fallback-service/v1/messages";
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient(apiUrl, "api-key-1", "token-2", mockRestTemplate, 1, 50);

        FallbackMessageRequest request = new FallbackMessageRequest("orders", "order-app", "{}", null);

        when(mockRestTemplate.exchange(eq(apiUrl), eq(HttpMethod.POST), any(HttpEntity.class), eq(FallbackApiResponse.class)))
                .thenReturn(new ResponseEntity<>(new FallbackApiResponse("m1", "BUFFERED"), HttpStatus.OK));

        client.send(request);

        ArgumentCaptor<HttpEntity> captor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(mockRestTemplate).exchange(eq(apiUrl), eq(HttpMethod.POST), captor.capture(), eq(FallbackApiResponse.class));

        HttpEntity capturedEntity = captor.getValue();
        assertEquals("api-key-1", capturedEntity.getHeaders().getFirst("X-API-Key"));
        assertEquals("token-2", capturedEntity.getHeaders().getFirst("token"));
        assertEquals("Bearer token-2", capturedEntity.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
    }

    @Test
    @DisplayName("Getters should expose configured properties")
    void testGetters() {
        RestTemplateFallbackHttpClient client = new RestTemplateFallbackHttpClient("http://api.test", "my-key", "my-token", null, 1000, 2000, 2, 100);
        assertEquals("http://api.test", client.getApiUrl());
        assertEquals("my-key", client.getApiKey());
        assertEquals("my-token", client.getToken());
        assertNotNull(client.getRestTemplate());
    }
}
