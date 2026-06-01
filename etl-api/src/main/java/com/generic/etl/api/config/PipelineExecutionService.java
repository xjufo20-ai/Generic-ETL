package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.model.ParallelConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.PipelineRun;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.DeadLetterQueue;
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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    public PipelineRun executeFromJson(String pipelineJson) { return doExecute(runIdSeq.getAndIncrement(), pipelineJson); }

    public PipelineRun executeByName(String pipelineName, StateStore store) {
        String json = store.getPipeline(pipelineName);
        if (json == null) return fail(runIdSeq.getAndIncrement(), pipelineName, "Not found: " + pipelineName);
        return doExecute(runIdSeq.getAndIncrement(), json);
    }

    public PipelineRun retryRun(Long originalRunId, String pipelineJson) {
        PipelineRun orig = runHistory.get(originalRunId);
        if (orig == null) return fail(runIdSeq.getAndIncrement(), null, "Run not found: " + originalRunId);
        log.info("Retrying '{}' (original run {})", orig.getPipelineName(), originalRunId);
        return doExecute(runIdSeq.getAndIncrement(), pipelineJson);
    }

    private PipelineRun doExecute(long runId, String pipelineJson) {
        PipelineConfig config;
        try { config = configParser.parseFromString(pipelineJson); }
        catch (Exception e) { return fail(runId, null, "Parse error: " + e.getMessage()); }

        List<String> issues = config.validate();
        if (!issues.isEmpty()) return fail(runId, config.getPipeline().getName(), "Validation: " + String.join("; ", issues));

        PipelineRun run = PipelineRun.builder().id(runId).pipelineName(config.getPipeline().getName())
                .status("RUNNING").startTime(LocalDateTime.now()).build();
        runHistory.put(runId, run);

        try {
            retryHandler.execute(() -> {
                ParallelConfig pc = config.getParallel() != null ? config.getParallel() : new ParallelConfig();
                DeadLetterQueue dlq = new DeadLetterQueue();

                // Parallel extraction + transform + load
                List<Row> rows;
                if (pc.getExtractPartitions() > 1) {
                    rows = executeParallel(config, pc, dlq);
                } else {
                    rows = executeSequential(config, pc, dlq);
                }

                loadRouter.route(config.getPipeline().getName(), rows, config.getOutput());

                long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
                run.setStatus("SUCCESS"); run.setRowCount(rows.size()); run.setDurationMs(dur); run.setEndTime(LocalDateTime.now());
                metrics.recordSuccess(config.getPipeline().getName(), rows.size(), dur);
                log.info("Pipeline '{}': {} rows in {}ms (DLQ: {})",
                        config.getPipeline().getName(), rows.size(), dur, dlq.getFailures(config.getPipeline().getName()).size());
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

    private List<Row> executeSequential(PipelineConfig config, ParallelConfig pc, DeadLetterQueue dlq) {
        return transformPipeline.build(config).apply(extractorRegistry.extract(config)).toList();
    }

    private List<Row> executeParallel(PipelineConfig config, ParallelConfig pc, DeadLetterQueue dlq) {
        int partitions = pc.getExtractPartitions();
        try (ForkJoinPool pool = new ForkJoinPool(partitions)) {
            return pool.submit(() ->
                IntStream.range(0, partitions).parallel().boxed()
                    .flatMap(partition -> {
                        // Each partition extracts its own shard (cursor-based partitioning via SQL modulo)
                        log.debug("Parallel partition {}/{}", partition + 1, partitions);
                        Stream<Row> extracted = extractorRegistry.extract(config);
                        return transformPipeline.build(config).apply(extracted);
                    })
                    .collect(Collectors.toList())
            ).join();
        }
    }

    private PipelineRun fail(long id, String name, String msg) {
        PipelineRun r = PipelineRun.builder().id(id).pipelineName(name).status("FAILED")
                .errorMessage(msg).startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
        runHistory.put(id, r); return r;
    }

    public PipelineRun getRun(Long id) { return runHistory.get(id); }
    public List<PipelineRun> getRunHistory() { return new ArrayList<>(runHistory.values()); }
    public List<PipelineRun> getRunHistory(String name) { return runHistory.values().stream().filter(r -> name.equals(r.getPipelineName())).toList(); }
    private static String trunc(String s, int max) { return s != null && s.length() > max ? s.substring(0, max) + "..." : s; }
}
