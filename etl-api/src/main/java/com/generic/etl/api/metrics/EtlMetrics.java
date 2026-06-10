package com.generic.etl.api.metrics;

import com.generic.etl.core.metrics.MetricsRecorder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class EtlMetrics implements MetricsRecorder {
    private final MeterRegistry registry;
    private Counter pipelinesExecuted;
    private Counter pipelinesFailed;
    private Counter rowsExtracted;
    private Timer pipelineDuration;

    public EtlMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    void init() {
        this.pipelinesExecuted = Counter.builder("etl.pipelines.executed")
                .description("Total number of successful pipeline executions").register(registry);
        this.pipelinesFailed = Counter.builder("etl.pipelines.failed")
                .description("Total number of failed pipeline executions").register(registry);
        this.rowsExtracted = Counter.builder("etl.rows.extracted")
                .description("Total number of rows extracted").register(registry);
        this.pipelineDuration = Timer.builder("etl.pipeline.duration")
                .description("Pipeline execution duration").register(registry);
    }

    @Override
    public void recordSuccess(String pipeline, long rows, long durationMs) {
        pipelinesExecuted.increment();
        rowsExtracted.increment(rows);
        pipelineDuration.record(durationMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordFailure(String pipeline) {
        pipelinesFailed.increment();
    }
}
