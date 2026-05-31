package com.generic.etl.extract.adapter.impl;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.extract.adapter.Extractor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Spliterators;
import java.util.Spliterator;

@Slf4j
public class CsvExtractor implements Extractor {

    @Override
    public boolean supports(PipelineConfig config) {
        return config.getDatasource() instanceof DataSourceConfig.CsvDataSource;
    }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig.CsvDataSource ds = (DataSourceConfig.CsvDataSource) config.getDatasource();
        Path path = Path.of(ds.getFilePath());
        String delimiter = ds.getDelimiter() != null ? ds.getDelimiter() : ",";

        try {
            BufferedReader reader = Files.newBufferedReader(path);
            List<String> headers;

            if (ds.isHasHeader()) {
                String headerLine = reader.readLine();
                headers = headerLine != null ? parseLine(headerLine, delimiter) : List.of();
            } else {
                headers = config.getInputSchema().getFields().stream()
                        .map(f -> f.getName()).toList();
            }

            List<String> finalHeaders = headers;

            Spliterator<Row> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.NONNULL) {
                @Override
                public boolean tryAdvance(java.util.function.Consumer<? super Row> action) {
                    try {
                        String line = reader.readLine();
                        if (line == null || line.isBlank()) return false;
                        List<String> values = parseLine(line, delimiter);
                        Map<String, Object> map = new LinkedHashMap<>();
                        for (int i = 0; i < finalHeaders.size(); i++)
                            map.put(finalHeaders.get(i), i < values.size() ? values.get(i) : null);
                        action.accept(new Row(map));
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException("CSV parse error", e);
                    }
                }
            };

            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> { try { reader.close(); } catch (Exception e) { log.warn("Failed to close CSV reader", e); } });
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV: " + ds.getFilePath(), e);
        }
    }

    private List<String> parseLine(String line, String delimiter) {
        return Arrays.asList(line.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .stream().map(s -> s.replaceAll("^\"|\"$", "").trim()).toList();
    }
}
