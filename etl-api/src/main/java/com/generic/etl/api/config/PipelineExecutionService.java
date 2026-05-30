package com.generic.etl.api.config;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.PipelineRun;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;
import com.generic.etl.extract.adapter.CamelExtractAdapter;
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
    private final CamelExtractAdapter extractAdapter;
    private final LoadRouter loadRouter;
    private final Map<Long, PipelineRun> runHistory = new ConcurrentHashMap<>();
    private final AtomicLong runIdSeq = new AtomicLong(1);

    private int maxRetries = DEFAULT_MAX_RETRIES;

    public PipelineExecutionService(PipelineConfigParser configParser,
                                     TransformChain transformChain,
                                     CamelExtractAdapter extractAdapter,
                                     LoadRouter loadRouter) {
        this.configParser = configParser;
        this.transformChain = transformChain;
        this.extractAdapter = extractAdapter;
        this.loadRouter = loadRouter;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /** Execute a pipeline from JSON, with retry on failure. */
    public PipelineRun executeFromJson(String pipelineJson) {
        long runId = runIdSeq.getAndIncrement();
        return executeWithRetry(runId, pipelineJson, 0);
    }

    /** Execute a pipeline by name (lookup from store). */
    public PipelineRun executeByName(String pipelineName, Map<String, String> pipelineStore) {
        String json = pipelineStore.get(pipelineName);
        if (json == null) {
            long runId = runIdSeq.getAndIncrement();
            PipelineRun failed = PipelineRun.builder()
                    .id(runId)
                    .pipelineName(pipelineName)
                    .status("FAILED")
                    .errorMessage("Pipeline not found in store: " + pipelineName)
                    .startTime(LocalDateTime.now())
                    .endTime(LocalDateTime.now())
                    .build();
            runHistory.put(runId, failed);
            return failed;
        }
        long runId = runIdSeq.getAndIncrement();
        return executeWithRetry(runId, json, 0);
    }

    /** Retry a previously failed run. */
    public PipelineRun retryRun(Long originalRunId, String pipelineJson) {
        PipelineRun original = runHistory.get(originalRunId);
        if (original == null) {
            long newId = runIdSeq.getAndIncrement();
            PipelineRun failed = PipelineRun.builder()
                    .id(newId)
                    .status("FAILED")
                    .errorMessage("Original run not found: " + originalRunId)
                    .startTime(LocalDateTime.now())
                    .endTime(LocalDateTime.now())
                    .build();
            runHistory.put(newId, failed);
            return failed;
        }
        long newRunId = runIdSeq.getAndIncrement();
        log.info("Retrying pipeline '{}' (original run {})", original.getPipelineName(), originalRunId);
        return executeWithRetry(newRunId, pipelineJson, 0);
    }

    private PipelineRun executeWithRetry(long runId, String pipelineJson, int attempt) {
        PipelineConfig config;
        try {
            config = configParser.parseFromString(pipelineJson);
        } catch (Exception e) {
            PipelineRun failed = PipelineRun.builder()
                    .id(runId)
                    .status("FAILED")
                    .errorMessage("Config parse error: " + e.getMessage())
                    .startTime(LocalDateTime.now())
                    .endTime(LocalDateTime.now())
                    .build();
            runHistory.put(runId, failed);
            return failed;
        }

        PipelineRun run = PipelineRun.builder()
                .id(runId)
                .pipelineName(config.getPipeline().getName())
                .status("RUNNING")
                .startTime(LocalDateTime.now())
                .build();
        runHistory.put(runId, run);

        try {
            // Extract via Camel adapter — single adapter handles all source types
            var extracted = extractAdapter.extract(config);
            var transformed = transformChain.apply(extracted, config);
            List<Row> rows = transformed.toList();

            loadRouter.route(config.getPipeline().getName(), rows, config.getOutput());

            long duration = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("SUCCESS");
            run.setRowCount(rows.size());
            run.setDurationMs(duration);
            run.setEndTime(LocalDateTime.now());
            log.info("Pipeline '{}' succeeded: {} rows in {}ms (attempt {})",
                    config.getPipeline().getName(), rows.size(), duration, attempt + 1);
        } catch (Exception e) {
            log.error("Pipeline '{}' failed (attempt {}/{}): {}",
                    config.getPipeline().getName(), attempt + 1, maxRetries + 1, e.getMessage());

            if (attempt < maxRetries) {
                long backoffMs = BASE_BACKOFF_MS * (long) Math.pow(2, attempt);
                log.info("Retrying pipeline '{}' in {}ms...", config.getPipeline().getName(), backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                runHistory.remove(runId);
                return executeWithRetry(runId, pipelineJson, attempt + 1);
            }

            long duration = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("FAILED");
            run.setDurationMs(duration);
            run.setEndTime(LocalDateTime.now());
            run.setErrorMessage(truncate(e.getMessage(), 2000));
        }

        runHistory.put(runId, run);
        return run;
    }

    public PipelineRun getRun(Long runId) {
        return runHistory.get(runId);
    }

    public List<PipelineRun> getRunHistory() {
        return new ArrayList<>(runHistory.values());
    }

    public List<PipelineRun> getRunHistory(String pipelineName) {
        return runHistory.values().stream()
                .filter(r -> pipelineName.equals(r.getPipelineName()))
                .toList();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
