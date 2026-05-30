package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;

import java.util.List;
import java.util.stream.Stream;

public class ExtractorRegistry {
    private final List<Extractor> extractors;

    public ExtractorRegistry(List<Extractor> extractors) {
        this.extractors = extractors;
    }

    public Stream<Row> extract(PipelineConfig config) {
        for (Extractor ex : extractors) {
            if (ex.supports(config)) {
                return ex.extract(config);
            }
        }
        throw new IllegalArgumentException("No extractor found for datasource type: " +
                config.getDatasource().getType());
    }
}
