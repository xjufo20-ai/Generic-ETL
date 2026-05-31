package com.generic.etl.api.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class EtlMetrics {
    private final Counter pipelinesExecuted;
    private final Counter pipelinesFailed;
    private final Counter rowsExtracted;
    private final Timer pipelineDuration;

    public EtlMetrics(MeterRegistry registry) {
        this.pipelinesExecuted = Counter.builder("etl.pipelines.executed")
                .description("Total number of successful pipeline executions").register(registry);
        this.pipelinesFailed = Counter.builder("etl.pipelines.failed")
                .description("Total number of failed pipeline executions").register(registry);
        this.rowsExtracted = Counter.builder("etl.rows.extracted")
                .description("Total number of rows extracted").register(registry);
        this.pipelineDuration = Timer.builder("etl.pipeline.duration")
                .description("Pipeline execution duration").register(registry);
    }

    public void recordSuccess(String pipeline, long rows, long durationMs) {
        pipelinesExecuted.increment();
        rowsExtracted.increment(rows);
        pipelineDuration.record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordFailure(String pipeline) {
        pipelinesFailed.increment();
    }
}
