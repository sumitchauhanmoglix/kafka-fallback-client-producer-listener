package com.moglix.kafka.fallback.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe, zero-dependency circuit breaker using atomic state transitions.
 */
public class SimpleCircuitBreaker implements CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(SimpleCircuitBreaker.class);

    private final int failureThreshold;
    private final long waitDurationInOpenMs;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong lastStateChangeTimestamp = new AtomicLong(System.currentTimeMillis());

    public SimpleCircuitBreaker(int failureThreshold, long waitDurationInOpenMs) {
        this.failureThreshold = Math.max(1, failureThreshold);
        this.waitDurationInOpenMs = Math.max(100, waitDurationInOpenMs);
    }

    @Override
    public boolean allowRequest() {
        State current = state.get();
        if (current == State.CLOSED) {
            return true;
        }

        if (current == State.OPEN) {
            long elapsed = System.currentTimeMillis() - lastStateChangeTimestamp.get();
            if (elapsed >= waitDurationInOpenMs) {
                // Time window passed -> attempt probe transition to HALF_OPEN
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    lastStateChangeTimestamp.set(System.currentTimeMillis());
                    log.info("Circuit breaker transitioned from OPEN to HALF_OPEN (probing Kafka)");
                    return true;
                }
            }
            return false;
        }

        // In HALF_OPEN, only allow the initial probe request, reject concurrent calls
        return false;
    }

    @Override
    public void recordSuccess() {
        consecutiveFailures.set(0);
        State prev = state.getAndSet(State.CLOSED);
        if (prev != State.CLOSED) {
            lastStateChangeTimestamp.set(System.currentTimeMillis());
            log.info("Circuit breaker recovered: transitioned from {} to CLOSED", prev);
        }
    }

    @Override
    public void recordFailure() {
        State current = state.get();
        if (current == State.HALF_OPEN) {
            // Probe failed, immediately reopen circuit
            state.set(State.OPEN);
            lastStateChangeTimestamp.set(System.currentTimeMillis());
            log.warn("Probe request failed in HALF_OPEN: circuit breaker reopening to OPEN");
            return;
        }

        if (current == State.CLOSED) {
            int failures = consecutiveFailures.incrementAndGet();
            if (failures >= failureThreshold) {
                if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                    lastStateChangeTimestamp.set(System.currentTimeMillis());
                    log.error("Circuit breaker tripped: failure threshold reached ({} consecutive failures). State is now OPEN", failures);
                }
            }
        }
    }

    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public void reset() {
        consecutiveFailures.set(0);
        state.set(State.CLOSED);
        lastStateChangeTimestamp.set(System.currentTimeMillis());
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }
}
