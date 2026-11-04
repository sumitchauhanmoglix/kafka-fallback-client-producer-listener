package com.moglix.kafka.fallback.properties;

import java.io.Serializable;

/**
 * Common configuration properties for kafka.fallback.*
 */
public class KafkaFallbackProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Whether Kafka fallback client is enabled.
     */
    private boolean enabled = true;

    /**
     * Name of the calling application. Defaults to spring.application.name if empty.
     */
    private String applicationName;

    /**
     * Target fallback endpoint (e.g. http://fallback-service/v1/messages).
     */
    private String apiUrl;

    /**
     * Optional API key header value (sent as X-API-Key).
     */
    private String apiKey;

    /**
     * Timeout configurations.
     */
    private Timeouts timeouts = new Timeouts();

    /**
     * Circuit breaker configurations.
     */
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();

    /**
     * Retry configurations for Fallback HTTP calls.
     */
    private RetryProperties retry = new RetryProperties();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public Timeouts getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(Timeouts timeouts) {
        this.timeouts = timeouts;
    }

    public CircuitBreakerProperties getCircuitBreaker() {
        return circuitBreaker;
    }

    public void setCircuitBreaker(CircuitBreakerProperties circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public RetryProperties getRetry() {
        return retry;
    }

    public void setRetry(RetryProperties retry) {
        this.retry = retry;
    }

    public static class Timeouts implements Serializable {
        private static final long serialVersionUID = 1L;

        private long kafkaSendTimeoutMs = 2000L;
        private int apiConnectTimeoutMs = 1000;
        private int apiReadTimeoutMs = 3000;

        public long getKafkaSendTimeoutMs() {
            return kafkaSendTimeoutMs;
        }

        public void setKafkaSendTimeoutMs(long kafkaSendTimeoutMs) {
            this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
        }

        public int getApiConnectTimeoutMs() {
            return apiConnectTimeoutMs;
        }

        public void setApiConnectTimeoutMs(int apiConnectTimeoutMs) {
            this.apiConnectTimeoutMs = apiConnectTimeoutMs;
        }

        public int getApiReadTimeoutMs() {
            return apiReadTimeoutMs;
        }

        public void setApiReadTimeoutMs(int apiReadTimeoutMs) {
            this.apiReadTimeoutMs = apiReadTimeoutMs;
        }
    }

    public static class CircuitBreakerProperties implements Serializable {
        private static final long serialVersionUID = 1L;

        private int failureThreshold = 5;
        private long waitDurationInOpenMs = 10000L;

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public long getWaitDurationInOpenMs() {
            return waitDurationInOpenMs;
        }

        public void setWaitDurationInOpenMs(long waitDurationInOpenMs) {
            this.waitDurationInOpenMs = waitDurationInOpenMs;
        }
    }

    public static class RetryProperties implements Serializable {
        private static final long serialVersionUID = 1L;

        private int maxAttempts = 2;
        private long backoffMs = 200L;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getBackoffMs() {
            return backoffMs;
        }

        public void setBackoffMs(long backoffMs) {
            this.backoffMs = backoffMs;
        }
    }
}
