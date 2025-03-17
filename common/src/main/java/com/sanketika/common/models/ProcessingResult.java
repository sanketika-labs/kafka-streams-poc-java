package com.sanketika.common.models;

import java.util.Map;

public class ProcessingResult {
    private String payload;
    private boolean isSuccess;
    private ErrorEvent error;
    private Map<String, Object> metadata;

    public ProcessingResult() {
    }

    public ProcessingResult(String payload, boolean isSuccess, ErrorEvent error) {
        this.payload = payload;
        this.isSuccess = isSuccess;
        this.error = error;
        this.metadata = null;
    }

    public ProcessingResult(String payload, boolean isSuccess, ErrorEvent error, Map<String, Object> metadata) {
        this.payload = payload;
        this.isSuccess = isSuccess;
        this.error = error;
        this.metadata = metadata;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public ErrorEvent getError() {
        return error;
    }

    public void setError(ErrorEvent error) {
        this.error = error;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
