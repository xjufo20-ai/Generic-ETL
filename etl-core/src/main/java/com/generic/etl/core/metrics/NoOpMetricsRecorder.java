package com.generic.etl.core.metrics;

/** Default no-op recorder when no real metrics backend is configured. */
public class NoOpMetricsRecorder implements MetricsRecorder {
    @Override public void recordSuccess(String pipeline, long rows, long durationMs) {}
    @Override public void recordFailure(String pipeline) {}
}
