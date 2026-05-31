package com.generic.etl.api.config;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.PipelineRun;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;
import com.generic.etl.api.metrics.EtlMetrics;
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
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long BASE_BACKOFF_MS = 1000;

    private final PipelineConfigParser configParser;
    private final TransformChain transformChain;
    private final ExtractorRegistry extractorRegistry;
    private final LoadRouter loadRouter;
    private final EtlMetrics metrics;
    private final Map<Long, PipelineRun> runHistory = new ConcurrentHashMap<>();
    private final AtomicLong runIdSeq = new AtomicLong(1);

    private int maxRetries = DEFAULT_MAX_RETRIES;

    public PipelineExecutionService(PipelineConfigParser configParser,
                                     TransformChain transformChain,
                                     ExtractorRegistry extractorRegistry,
                                     LoadRouter loadRouter, EtlMetrics metrics) {
        this.configParser = configParser;
        this.transformChain = transformChain;
        this.extractorRegistry = extractorRegistry;
        this.loadRouter = loadRouter;
        this.metrics = metrics;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public PipelineRun executeFromJson(String pipelineJson) {
        long runId = runIdSeq.getAndIncrement();
        return executeWithRetry(runId, pipelineJson, 0);
    }

    public PipelineRun executeByName(String pipelineName, Map<String, String> pipelineStore) {
        String json = pipelineStore.get(pipelineName);
        if (json == null) {
            long runId = runIdSeq.getAndIncrement();
            PipelineRun failed = PipelineRun.builder()
                    .id(runId).pipelineName(pipelineName).status("FAILED")
                    .errorMessage("Pipeline not found: " + pipelineName)
                    .startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
            runHistory.put(runId, failed);
            return failed;
        }
        return executeWithRetry(runIdSeq.getAndIncrement(), json, 0);
    }

    public PipelineRun retryRun(Long originalRunId, String pipelineJson) {
        PipelineRun original = runHistory.get(originalRunId);
        if (original == null) {
            long id = runIdSeq.getAndIncrement();
            PipelineRun failed = PipelineRun.builder()
                    .id(id).status("FAILED").errorMessage("Original run not found: " + originalRunId)
                    .startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
            runHistory.put(id, failed);
            return failed;
        }
        log.info("Retrying pipeline '{}' (original run {})", original.getPipelineName(), originalRunId);
        return executeWithRetry(runIdSeq.getAndIncrement(), pipelineJson, 0);
    }

    private PipelineRun executeWithRetry(long runId, String pipelineJson, int attempt) {
        PipelineConfig config;
        try {
            config = configParser.parseFromString(pipelineJson);
        } catch (Exception e) {
            PipelineRun failed = PipelineRun.builder()
                    .id(runId).status("FAILED").errorMessage("Config parse error: " + e.getMessage())
                    .startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
            runHistory.put(runId, failed);
            return failed;
        }

        List<String> validationIssues = config.validate();
        if (!validationIssues.isEmpty()) {
            PipelineRun failed = PipelineRun.builder()
                    .id(runId).pipelineName(config.getPipeline().getName()).status("FAILED")
                    .errorMessage("Validation failed: " + String.join("; ", validationIssues))
                    .startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
            runHistory.put(runId, failed);
            return failed;
        }

        PipelineRun run = PipelineRun.builder()
                .id(runId).pipelineName(config.getPipeline().getName())
                .status("RUNNING").startTime(LocalDateTime.now()).build();
        runHistory.put(runId, run);

        try {
            var extracted = extractorRegistry.extract(config);
            var transformed = transformChain.apply(extracted, config);
            List<Row> rows = transformed.toList();

            loadRouter.route(config.getPipeline().getName(), rows, config.getOutput());

            long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("SUCCESS");
            run.setRowCount(rows.size());
            run.setDurationMs(dur);
            run.setEndTime(LocalDateTime.now());
            metrics.recordSuccess(config.getPipeline().getName(), rows.size(), dur);
            log.info("Pipeline '{}': {} rows in {}ms", config.getPipeline().getName(), rows.size(), dur);
        } catch (Exception e) {
            metrics.recordFailure(config.getPipeline().getName());
            log.error("Pipeline '{}' failed (attempt {}/{}): {}", config.getPipeline().getName(), attempt + 1, maxRetries + 1, e.getMessage());

            if (attempt < maxRetries) {
                long backoff = BASE_BACKOFF_MS * (long) Math.pow(2, attempt);
                log.info("Retrying in {}ms...", backoff);
                try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                runHistory.remove(runId);
                return executeWithRetry(runId, pipelineJson, attempt + 1);
            }

            long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("FAILED");
            run.setDurationMs(dur);
            run.setEndTime(LocalDateTime.now());
            run.setErrorMessage(truncate(e.getMessage(), 2000));
        }

        runHistory.put(runId, run);
        return run;
    }

    public PipelineRun getRun(Long runId) { return runHistory.get(runId); }

    public List<PipelineRun> getRunHistory() { return new ArrayList<>(runHistory.values()); }

    public List<PipelineRun> getRunHistory(String pipelineName) {
        return runHistory.values().stream().filter(r -> pipelineName.equals(r.getPipelineName())).toList();
    }

    private static String truncate(String s, int max) {
        return s != null && s.length() > max ? s.substring(0, max) + "..." : s;
    }
}
