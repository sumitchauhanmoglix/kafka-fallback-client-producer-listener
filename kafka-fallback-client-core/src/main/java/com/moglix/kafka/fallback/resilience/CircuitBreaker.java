package com.moglix.kafka.fallback.resilience;

/**
 * Interface defining circuit breaker operations for resilient routing.
 */
public interface CircuitBreaker {

    enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    /**
     * Determines whether an outgoing call to Kafka is permitted.
     *
     * @return true if permitted, false if the circuit is open
     */
    boolean allowRequest();

    /**
     * Records a successful execution.
     */
    void recordSuccess();

    /**
     * Records a failed execution.
     */
    void recordFailure();

    /**
     * Current state of the circuit breaker.
     */
    State getState();

    /**
     * Resets the circuit breaker to CLOSED.
     */
    void reset();
}
