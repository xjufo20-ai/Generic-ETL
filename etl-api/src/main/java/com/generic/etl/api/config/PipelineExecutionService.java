package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.PipelineRun;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformPipeline;
import com.generic.etl.extract.adapter.ExtractorRegistry;
import com.generic.etl.load.LoadRouter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class PipelineExecutionService {
    private final PipelineConfigParser configParser;
    private final TransformPipeline transformPipeline;
    private final ExtractorRegistry extractorRegistry;
    private final LoadRouter loadRouter;
    private final EtlMetrics metrics;
    private final RetryHandler retryHandler;
    private final Map<Long, PipelineRun> runHistory = new ConcurrentHashMap<>();
    private final AtomicLong runIdSeq = new AtomicLong(1);

    public PipelineExecutionService(PipelineConfigParser configParser, TransformPipeline transformPipeline,
                                     ExtractorRegistry extractorRegistry, LoadRouter loadRouter, EtlMetrics metrics) {
        this(configParser, transformPipeline, extractorRegistry, loadRouter, metrics, new RetryHandler());
    }

    public PipelineExecutionService(PipelineConfigParser configParser, TransformPipeline transformPipeline,
                                     ExtractorRegistry extractorRegistry, LoadRouter loadRouter,
                                     EtlMetrics metrics, RetryHandler retryHandler) {
        this.configParser = configParser;
        this.transformPipeline = transformPipeline;
        this.extractorRegistry = extractorRegistry;
        this.loadRouter = loadRouter;
        this.metrics = metrics;
        this.retryHandler = retryHandler;
    }

    public PipelineRun executeFromJson(String pipelineJson) {
        long runId = runIdSeq.getAndIncrement();
        return doExecute(runId, pipelineJson);
    }

    public PipelineRun executeByName(String pipelineName, StateStore store) {
        String json = store.getPipeline(pipelineName);
        if (json == null) return failFast(runIdSeq.getAndIncrement(), pipelineName, "Pipeline not found: " + pipelineName);
        return doExecute(runIdSeq.getAndIncrement(), json);
    }

    public PipelineRun retryRun(Long originalRunId, String pipelineJson) {
        PipelineRun original = runHistory.get(originalRunId);
        if (original == null) return failFast(runIdSeq.getAndIncrement(), null, "Run not found: " + originalRunId);
        log.info("Retrying '{}' (original run {})", original.getPipelineName(), originalRunId);
        return doExecute(runIdSeq.getAndIncrement(), pipelineJson);
    }

    private PipelineRun doExecute(long runId, String pipelineJson) {
        PipelineConfig config;
        try { config = configParser.parseFromString(pipelineJson); }
        catch (Exception e) { return failFast(runId, null, "Config parse error: " + e.getMessage()); }

        List<String> issues = config.validate();
        if (!issues.isEmpty()) return failFast(runId, config.getPipeline().getName(), "Validation: " + String.join("; ", issues));

        PipelineRun run = PipelineRun.builder().id(runId).pipelineName(config.getPipeline().getName())
                .status("RUNNING").startTime(LocalDateTime.now()).build();
        runHistory.put(runId, run);

        try {
            retryHandler.execute(() -> {
                var rows = transformPipeline.build(config).apply(extractorRegistry.extract(config)).toList();
                loadRouter.route(config.getPipeline().getName(), rows, config.getOutput());
                long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
                run.setStatus("SUCCESS"); run.setRowCount(rows.size()); run.setDurationMs(dur); run.setEndTime(LocalDateTime.now());
                metrics.recordSuccess(config.getPipeline().getName(), rows.size(), dur);
                log.info("Pipeline '{}': {} rows in {}ms", config.getPipeline().getName(), rows.size(), dur);
                return null;
            }, config.getPipeline().getName());
        } catch (Exception e) {
            long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("FAILED"); run.setDurationMs(dur); run.setEndTime(LocalDateTime.now());
            run.setErrorMessage(trunc(e.getMessage(), 2000));
            metrics.recordFailure(config.getPipeline().getName());
        }
        runHistory.put(runId, run);
        return run;
    }

    private PipelineRun failFast(long id, String name, String msg) {
        PipelineRun r = PipelineRun.builder().id(id).pipelineName(name).status("FAILED")
                .errorMessage(msg).startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
        runHistory.put(id, r);
        return r;
    }

    public PipelineRun getRun(Long id) { return runHistory.get(id); }
    public List<PipelineRun> getRunHistory() { return new ArrayList<>(runHistory.values()); }
    public List<PipelineRun> getRunHistory(String name) { return runHistory.values().stream().filter(r -> name.equals(r.getPipelineName())).toList(); }
    private static String trunc(String s, int max) { return s != null && s.length() > max ? s.substring(0, max) + "..." : s; }
}
