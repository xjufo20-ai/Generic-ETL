package com.generic.etl.extract.adapter;

import lombok.extern.slf4j.Slf4j;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
                headers = parseLine(headerLine, delimiter);
            } else {
                // Use input schema field names as headers
                headers = config.getInputSchema().getFields().stream()
                        .map(f -> f.getName())
                        .toList();
            }

            Spliterator<Row> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.NONNULL) {
                @Override
                public boolean tryAdvance(Consumer<? super Row> action) {
                    try {
                        String line = reader.readLine();
                        if (line == null) return false;
                        List<String> values = parseLine(line, delimiter);
                        Map<String, Object> rowMap = new LinkedHashMap<>();
                        for (int i = 0; i < headers.size(); i++) {
                            Object val = i < values.size() ? values.get(i) : null;
                            rowMap.put(headers.get(i), val);
                        }
                        action.accept(new Row(rowMap));
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException("CSV parse error", e);
                    }
                }
            };

            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> { try { reader.close(); } catch (Exception e) { log.warn("Failed to close CSV reader", e); } });
        } catch (Exception e) {
            throw new RuntimeException("Failed to open CSV file: " + ds.getFilePath(), e);
        }
    }

    private List<String> parseLine(String line, String delimiter) {
        // Simple CSV parsing (handles basic quoted fields)
        return Arrays.asList(line.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .stream()
                .map(s -> s.replaceAll("^\"|\"$", "").trim())
                .toList();
    }
}
