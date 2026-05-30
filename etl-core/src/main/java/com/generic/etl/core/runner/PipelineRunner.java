package com.generic.etl.core.runner;

import lombok.extern.slf4j.Slf4j;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class PipelineRunner {

    private final PipelineConfigParser configParser;
    private final TransformChain transformChain;
    private final Function<PipelineConfig, Stream<Row>> extractorProvider;
    private final Function<List<Row>, Void> loaderCallback;

    public PipelineRunner(
            PipelineConfigParser configParser,
            TransformChain transformChain,
            Function<PipelineConfig, Stream<Row>> extractorProvider,
            Function<List<Row>, Void> loaderCallback) {
        this.configParser = configParser;
        this.transformChain = transformChain;
        this.extractorProvider = extractorProvider;
        this.loaderCallback = loaderCallback;
    }

    public PipelineRunResult run(String configJson) {
        long start = System.currentTimeMillis();
        PipelineRunResult result = new PipelineRunResult();

        try {
            PipelineConfig config = configParser.parseFromString(configJson);
            result.pipelineName = config.getPipeline().getName();

            // Extract
            Stream<Row> extracted = extractorProvider.apply(config);

            // Transform
            Stream<Row> transformed = transformChain.apply(extracted, config);

            // Collect and load
            List<Row> rows = transformed.toList();
            result.rowCount = rows.size();

            // Load
            loaderCallback.apply(rows);

            result.success = true;
        } catch (Exception e) {
            log.error("Pipeline execution failed", e);
            result.success = false;
            result.errorMessage = e.getMessage();
        }

        result.durationMs = System.currentTimeMillis() - start;
        log.info("Pipeline '{}' completed: success={}, rows={}, duration={}ms",
                result.pipelineName, result.success, result.rowCount, result.durationMs);

        return result;
    }

    public static class PipelineRunResult {
        public String pipelineName;
        public boolean success;
        public long rowCount;
        public long durationMs;
        public String errorMessage;
    }
}
