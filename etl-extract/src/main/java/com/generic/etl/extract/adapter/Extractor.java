package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;

import java.util.stream.Stream;

public interface Extractor {
    /** Returns true if this extractor can handle the given config. */
    boolean supports(PipelineConfig config);

    /** Extract data as a stream of rows. */
    Stream<Row> extract(PipelineConfig config);
}
