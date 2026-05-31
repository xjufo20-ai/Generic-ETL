package com.generic.etl.api.config;

import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

/**
 * Standalone retry logic with exponential backoff.
 * Extracted from PipelineExecutionService for clarity and testability.
 */
@Slf4j
public class RetryHandler {
    private final int maxRetries;
    private final long baseBackoffMs;

    public RetryHandler() { this(3, 1000); }
    public RetryHandler(int maxRetries, long baseBackoffMs) {
        this.maxRetries = maxRetries;
        this.baseBackoffMs = baseBackoffMs;
    }

    public <T> T execute(Supplier<T> action, String taskName) {
        Exception lastEx = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return action.get();
            } catch (Exception e) {
                lastEx = e;
                if (attempt < maxRetries) {
                    long backoff = baseBackoffMs * (long) Math.pow(2, attempt);
                    log.warn("{} failed (attempt {}/{}), retrying in {}ms: {}",
                            taskName, attempt + 1, maxRetries + 1, backoff, e.getMessage());
                    try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
        throw new RuntimeException(taskName + " failed after " + (maxRetries + 1) + " attempts", lastEx);
    }
}
