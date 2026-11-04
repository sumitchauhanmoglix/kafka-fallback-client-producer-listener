package com.moglix.kafka.fallback.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

/**
 * RestTemplate implementation of FallbackHttpClient.
 * Configures timeouts, retries, and wires the shared ObjectMapper into RestTemplate's message converters.
 */
public class RestTemplateFallbackHttpClient implements FallbackHttpClient {

    private static final Logger log = LoggerFactory.getLogger(RestTemplateFallbackHttpClient.class);

    private final String apiUrl;
    private final String apiKey;
    private final RestTemplate restTemplate;
    private final int maxRetries;
    private final long retryBackoffMs;

    public RestTemplateFallbackHttpClient(String apiUrl, String apiKey, int connectTimeoutMs, int readTimeoutMs) {
        this(apiUrl, apiKey, null, connectTimeoutMs, readTimeoutMs, 2, 200L);
    }

    public RestTemplateFallbackHttpClient(String apiUrl, String apiKey, int connectTimeoutMs, int readTimeoutMs, int maxRetries, long retryBackoffMs) {
        this(apiUrl, apiKey, null, connectTimeoutMs, readTimeoutMs, maxRetries, retryBackoffMs);
    }

    public RestTemplateFallbackHttpClient(String apiUrl,
                                          String apiKey,
                                          ObjectMapper objectMapper,
                                          int connectTimeoutMs,
                                          int readTimeoutMs,
                                          int maxRetries,
                                          long retryBackoffMs) {
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
        this.maxRetries = Math.max(1, maxRetries);
        this.retryBackoffMs = Math.max(50, retryBackoffMs);

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Math.max(100, connectTimeoutMs));
        factory.setReadTimeout(Math.max(100, readTimeoutMs));
        this.restTemplate = new RestTemplate(factory);

        if (objectMapper != null) {
            boolean converterConfigured = false;
            for (HttpMessageConverter<?> converter : this.restTemplate.getMessageConverters()) {
                if (converter instanceof MappingJackson2HttpMessageConverter) {
                    ((MappingJackson2HttpMessageConverter) converter).setObjectMapper(objectMapper);
                    converterConfigured = true;
                    break;
                }
            }
            if (!converterConfigured) {
                this.restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter(objectMapper));
            }
        }
    }

    // Constructor for testing with mock RestTemplate
    public RestTemplateFallbackHttpClient(String apiUrl, String apiKey, RestTemplate restTemplate, int maxRetries, long retryBackoffMs) {
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
        this.restTemplate = restTemplate;
        this.maxRetries = Math.max(1, maxRetries);
        this.retryBackoffMs = Math.max(50, retryBackoffMs);
    }

    @Override
    public FallbackApiResponse send(FallbackMessageRequest request) throws Exception {
        if (apiUrl == null || apiUrl.trim().isEmpty()) {
            throw new IllegalStateException("Fallback API URL is not configured (kafka.fallback.api-url is empty)");
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        if (apiKey != null && !apiKey.trim().isEmpty()) {
            headers.set("X-API-Key", apiKey.trim());
        }

        HttpEntity<FallbackMessageRequest> entity = new HttpEntity<>(request, headers);

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Dispatching to fallback API (attempt {}/{}): {}", attempt, maxRetries, apiUrl);
                ResponseEntity<FallbackApiResponse> response = restTemplate.exchange(
                        apiUrl,
                        HttpMethod.POST,
                        entity,
                        FallbackApiResponse.class
                );

                if (response.getStatusCode().is2xxSuccessful()) {
                    FallbackApiResponse body = response.getBody();
                    if (body == null) {
                        body = new FallbackApiResponse(null, "BUFFERED");
                    }
                    log.info("Message successfully buffered via fallback API for topic '{}', messageId={}",
                            request.getTopic(), body.getMessageId());
                    return body;
                } else {
                    throw new RuntimeException("Fallback API returned HTTP status: " + response.getStatusCodeValue());
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("Fallback API call failed on attempt {}/{}: {}", attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(retryBackoffMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }
                }
            }
        }

        throw lastException != null ? lastException : new RuntimeException("Unknown error calling fallback API");
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }
}
