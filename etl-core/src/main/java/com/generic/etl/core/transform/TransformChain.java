package com.generic.etl.core.transform;

import lombok.extern.slf4j.Slf4j;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
class TransformChain {

    private final Map<String, TransformProcessor> processors;

    public TransformChain(Map<String, TransformProcessor> processors) {
        this.processors = processors;
    }

    /**
     * Apply all transforms to a stream of rows.
     * Row-level transforms (filter, rename, typeCast) are applied as stream.
     * Set-level transforms (aggregate) are applied after collecting the stream.
     */
    public Stream<Row> apply(Stream<Row> input, PipelineConfig config) {
        if (config.getTransforms() == null || config.getTransforms().isEmpty()) {
            return input;
        }

        Stream<Row> stream = input;
        List<Row> collected = null;

        for (TransformDef def : config.getTransforms()) {
            TransformProcessor processor = processors.get(def.getType());
            if (processor == null) {
                throw new IllegalArgumentException("Unknown transform type: " + def.getType());
            }

            if (processor.isSetProcessor()) {
                // Collect the stream for set-level processing
                if (collected == null) {
                    collected = stream.collect(Collectors.toList());
                }
                collected = processor.processSet(collected, def);
                log.debug("Applied set transform '{}': {} rows", def.getType(), collected.size());
            } else {
                if (collected != null) {
                    stream = collected.stream();
                    collected = null;
                }
                stream = processor.processStream(stream, def);
                log.debug("Applied row transform '{}'", def.getType());
            }
        }

        if (collected != null) {
            return collected.stream();
        }
        return stream;
    }
}
