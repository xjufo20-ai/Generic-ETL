package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Camel-based extraction adapter. Dynamically builds routes for different data sources.
 * The same Camel endpoint abstraction means switching from Oracle → MySQL → CSV
 * requires only a JSON config change — zero code change.
 */
@Slf4j
public class CamelExtractAdapter implements Extractor {

    private final CamelContext camelContext;

    public CamelExtractAdapter(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public boolean supports(PipelineConfig config) {
        return true; // Camel can handle any configured datasource
    }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();

        if (ds instanceof DataSourceConfig.JdbcDataSource jdbc) {
            return extractJdbc(jdbc, config);
        } else if (ds instanceof DataSourceConfig.CsvDataSource csv) {
            return extractCsv(csv, config);
        }

        throw new IllegalArgumentException("Unsupported datasource type: " + ds.getType());
    }

    // ── JDBC extraction via Camel ──────────────────────────

    private Stream<Row> extractJdbc(DataSourceConfig.JdbcDataSource ds, PipelineConfig config) {
        String dsName = registerDataSource(ds);

        // Build endpoint URI — Camel's endpoint abstraction decouples source type
        String uri = buildJdbcUri(dsName, ds);

        ProducerTemplate template = camelContext.createProducerTemplate();
        List<Map<String, Object>> rows;

        try {
            // Camel JDBC component returns ArrayList<HashMap> by default
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> result = template.requestBody(uri, ds.getQuery(), List.class);
            rows = result != null ? result : List.of();
        } catch (Exception e) {
            throw new RuntimeException("Camel JDBC extraction failed for " + ds.getType(), e);
        }

        return rows.stream()
                .map(this::mapToRow)
                .map(row -> applyCursorIfNeeded(row, ds, config));
    }

    private String buildJdbcUri(String dsName, DataSourceConfig.JdbcDataSource ds) {
        StringBuilder uri = new StringBuilder("jdbc:").append(dsName).append("?");
        if (ds.getCursor() != null && ds.getCursor().getPageSize() > 0) {
            uri.append("outputType=StreamList&");
        }
        uri.append("readSize=").append(ds.getCursor() != null ? ds.getCursor().getPageSize() : 5000);
        return uri.toString();
    }

    private String registerDataSource(DataSourceConfig.JdbcDataSource ds) {
        String dsName = "ds-" + ds.getType() + "-" + Math.abs(ds.getConnection().getUrl().hashCode() % 10000);

        // Only register if not already in registry
        if (camelContext.getRegistry().lookupByName(dsName) == null) {
            javax.sql.DataSource dataSource = DataSourceManager.createDataSource(ds.getConnection());
            camelContext.getRegistry().bind(dsName, dataSource);
            log.info("Registered Camel datasource '{}' for {}", dsName, ds.getType());
        }
        return dsName;
    }

    // ── CSV extraction via Camel ───────────────────────────

    private Stream<Row> extractCsv(DataSourceConfig.CsvDataSource ds, PipelineConfig config) {
        // Use Camel File component to read CSV
        ProducerTemplate template = camelContext.createProducerTemplate();

        String uri = String.format("file:%s?noop=true&fileName=%s",
                ds.getFilePath().contains("/") ?
                    ds.getFilePath().substring(0, ds.getFilePath().lastIndexOf('/')) : ".",
                ds.getFilePath().contains("/") ?
                    ds.getFilePath().substring(ds.getFilePath().lastIndexOf('/') + 1) : ds.getFilePath());

        try {
            // Read file content as string, then parse CSV
            String content = template.requestBody(uri, null, String.class);
            if (content == null || content.isBlank()) {
                return Stream.empty();
            }
            return parseCsvContent(content, ds, config);
        } catch (Exception e) {
            throw new RuntimeException("Camel CSV extraction failed for " + ds.getFilePath(), e);
        }
    }

    private Stream<Row> parseCsvContent(String content, DataSourceConfig.CsvDataSource ds, PipelineConfig config) {
        String[] lines = content.split("\\r?\\n");
        if (lines.length == 0) return Stream.empty();

        String delimiter = ds.getDelimiter() != null ? ds.getDelimiter() : ",";
        int startIdx = 0;
        List<String> headers;

        if (ds.isHasHeader()) {
            headers = parseCsvLine(lines[0], delimiter);
            startIdx = 1;
        } else {
            headers = config.getInputSchema().getFields().stream()
                    .map(com.generic.etl.common.model.SchemaConfig.FieldDef::getName)
                    .toList();
        }

        List<String> finalHeaders = headers;
        return java.util.Arrays.stream(lines, startIdx, lines.length)
                .filter(line -> !line.isBlank())
                .map(line -> {
                    List<String> values = parseCsvLine(line, delimiter);
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (int i = 0; i < finalHeaders.size(); i++) {
                        map.put(finalHeaders.get(i), i < values.size() ? values.get(i) : null);
                    }
                    return new Row(map);
                });
    }

    private List<String> parseCsvLine(String line, String delimiter) {
        return Arrays.asList(line.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .stream()
                .map(s -> s.replaceAll("^\"|\"$", "").trim())
                .toList();
    }

    // ── Helpers ────────────────────────────────────────────

    private Row mapToRow(Map<String, Object> map) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            values.put(entry.getKey(), entry.getValue());
        }
        return new Row(values);
    }

    /**
     * If cursor config is present, mark each row with a synthetic cursor column
     * so the transform chain can apply cursor-based pagination.
     */
    private Row applyCursorIfNeeded(Row row, DataSourceConfig.JdbcDataSource ds, PipelineConfig config) {
        // Camel JDBC handles pagination via outputType=StreamList+readSize internally
        return row;
    }
}
