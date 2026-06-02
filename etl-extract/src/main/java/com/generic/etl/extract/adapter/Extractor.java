package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;

import java.util.stream.Stream;

public interface Extractor {
    /** Returns true if this extractor can handle the given config. */
    boolean supports(PipelineConfig config);

    /** Extract data as a stream of rows (Java pipeline path). */
    Stream<Row> extract(PipelineConfig config);

    /** Build Camel endpoint URI from config (Camel route path). */
    default String buildEndpointUri(PipelineConfig config) {
        throw new UnsupportedOperationException("Camel endpoint not supported: " + getClass().getSimpleName());
    }
}
