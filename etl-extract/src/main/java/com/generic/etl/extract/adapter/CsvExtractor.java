package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

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
            List<String> allLines = Files.readAllLines(path);
            if (allLines.isEmpty()) return Stream.empty();

            List<String> headers;
            int startIdx;

            if (ds.isHasHeader()) {
                headers = parseLine(allLines.get(0), delimiter);
                startIdx = 1;
            } else {
                headers = config.getInputSchema().getFields().stream()
                        .map(f -> f.getName())
                        .toList();
                startIdx = 0;
            }

            List<String> finalHeaders = headers;
            return allLines.stream()
                    .skip(startIdx)
                    .filter(line -> !line.isBlank())
                    .map(line -> {
                        List<String> values = parseLine(line, delimiter);
                        Map<String, Object> map = new LinkedHashMap<>();
                        for (int i = 0; i < finalHeaders.size(); i++) {
                            map.put(finalHeaders.get(i), i < values.size() ? values.get(i) : null);
                        }
                        return new Row(map);
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV: " + ds.getFilePath(), e);
        }
    }

    private List<String> parseLine(String line, String delimiter) {
        return Arrays.asList(line.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .stream()
                .map(s -> s.replaceAll("^\"|\"$", "").trim())
                .toList();
    }
}
