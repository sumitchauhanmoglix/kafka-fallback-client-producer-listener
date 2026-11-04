package com.moglix.kafka.fallback.resilience;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleCircuitBreakerTest {

    @Test
    @DisplayName("Should initialize in CLOSED state and permit requests")
    void testInitialState() {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(3, 1000);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());
    }

    @Test
    @DisplayName("Should trip to OPEN when failure threshold is reached")
    void testTripToOpen() {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(3, 1000);

        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());

        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());

        cb.recordFailure(); // 3rd failure reaches threshold
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest()); // Must bypass Kafka
    }

    @Test
    @DisplayName("Should transition from OPEN to HALF_OPEN after wait duration elapses")
    void testOpenToHalfOpenTransition() throws InterruptedException {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(2, 200); // 200ms wait

        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());

        // Wait for open duration to elapse
        Thread.sleep(250);

        // First call after duration should be allowed as probe (transitions to HALF_OPEN)
        assertTrue(cb.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        // Concurrent calls while probe is in flight should not be allowed
        assertFalse(cb.allowRequest());
    }

    @Test
    @DisplayName("Successful probe in HALF_OPEN should recover circuit to CLOSED")
    void testRecoveryFromHalfOpen() throws InterruptedException {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(1, 100);

        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150);
        assertTrue(cb.allowRequest()); // probe allowed -> HALF_OPEN

        cb.recordSuccess(); // probe succeeds
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());
    }

    @Test
    @DisplayName("Failed probe in HALF_OPEN should immediately reopen circuit to OPEN")
    void testFailedProbeReopens() throws InterruptedException {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(1, 100);

        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150);
        assertTrue(cb.allowRequest()); // probe allowed -> HALF_OPEN

        cb.recordFailure(); // probe failed
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());
    }
}
