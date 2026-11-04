package com.moglix.kafka.fallback.exception;

/**
 * Thrown before any network call when topic or payload validation fails.
 */
public class InvalidMessageException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
