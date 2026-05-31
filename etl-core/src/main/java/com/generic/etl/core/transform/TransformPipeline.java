package com.generic.etl.core.transform;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Fluent pipeline executor that chains transforms on a stream of rows.
 * Usage: new TransformPipeline(processors).build(config).apply(rows)
 */
public class TransformPipeline {
    private final Map<String, TransformProcessor> processors;

    public TransformPipeline(Map<String, TransformProcessor> processors) {
        this.processors = processors;
    }

    /** Build a configured pipeline from config. */
    public ConfiguredPipeline build(PipelineConfig config) {
        return new ConfiguredPipeline(config);
    }

    public class ConfiguredPipeline {
        private final PipelineConfig config;

        ConfiguredPipeline(PipelineConfig config) { this.config = config; }

        /** Apply all transforms to the input stream. */
        public Stream<Row> apply(Stream<Row> input) {
            if (config.getTransforms() == null || config.getTransforms().isEmpty()) return input;
            return new TransformChain(processors).apply(input, config);
        }
    }
}
