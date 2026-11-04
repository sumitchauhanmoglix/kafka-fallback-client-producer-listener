package com.moglix.kafka.fallback.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Response DTO from Fallback API (POST /v1/messages).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FallbackApiResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonAlias({"id", "message_id"})
    private String messageId;

    private String status;

    public FallbackApiResponse() {
    }

    public FallbackApiResponse(String messageId, String status) {
        this.messageId = messageId;
        this.status = status;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FallbackApiResponse{" +
                "messageId='" + messageId + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
