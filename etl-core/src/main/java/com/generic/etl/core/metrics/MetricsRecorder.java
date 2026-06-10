package com.generic.etl.core.metrics;

/**
 * Pluggable metrics recorder. The default no-op implementation discards all metrics;
 * wire a real implementation (e.g. Micrometer) via Spring to enable monitoring.
 */
public interface MetricsRecorder {
    void recordSuccess(String pipeline, long rows, long durationMs);
    void recordFailure(String pipeline);
}
