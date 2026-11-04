package com.moglix.kafka.fallback.http;

import com.moglix.kafka.fallback.model.FallbackApiResponse;
import com.moglix.kafka.fallback.model.FallbackMessageRequest;

/**
 * Client interface for delivering messages to the HTTP fallback service.
 */
public interface FallbackHttpClient {

    /**
     * Posts the message to the fallback service.
     *
     * @param request the contract request DTO
     * @return the API response containing status and messageId
     * @throws Exception if HTTP call fails or returns non-2xx status
     */
    FallbackApiResponse send(FallbackMessageRequest request) throws Exception;
}
