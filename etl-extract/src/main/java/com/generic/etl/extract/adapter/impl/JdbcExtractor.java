package com.generic.etl.extract.adapter.impl;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.extract.adapter.DataSourceManager;
import com.generic.etl.extract.adapter.Extractor;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class JdbcExtractor implements Extractor {

    private final DataSourceManager dsManager;

    public JdbcExtractor(DataSourceManager dsManager) {
        this.dsManager = dsManager;
    }

    @Override
    public boolean supports(PipelineConfig config) {
        return config.getDatasource() instanceof DataSourceConfig.JdbcDataSource;
    }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig.JdbcDataSource ds = (DataSourceConfig.JdbcDataSource) config.getDatasource();
        JdbcTemplate jdbc = new JdbcTemplate(dsManager.getOrCreate(ds.getConnection()));
        jdbc.setFetchSize(500);

        if (ds.getCursor() != null) {
            return extractWithCursor(jdbc, ds);
        }
        return extractAll(jdbc, ds);
    }

    private Stream<Row> extractAll(JdbcTemplate jdbc, DataSourceConfig.JdbcDataSource ds) {
        List<Map<String, Object>> rows = jdbc.queryForList(ds.getQuery());
        log.debug("Extracted {} rows (full load)", rows.size());
        return rows.stream().map(JdbcExtractor::mapToRow);
    }

    private Stream<Row> extractWithCursor(JdbcTemplate jdbc, DataSourceConfig.JdbcDataSource ds) {
        String col = ds.getCursor().getColumn();
        int pageSize = ds.getCursor().getPageSize();
        return Stream.generate(new CursorSupplier(jdbc, ds.getQuery(), col, pageSize))
                .takeWhile(list -> !list.isEmpty())
                .flatMap(List::stream);
    }

    private static class CursorSupplier implements java.util.function.Supplier<List<Row>> {
        private final JdbcTemplate jdbc;
        private final String cursorSql;
        private final String cursorCol;
        private Object lastCursor;
        private int batchNo;

        CursorSupplier(JdbcTemplate jdbc, String baseSql, String cursorCol, int pageSize) {
            this.jdbc = jdbc;
            this.cursorCol = cursorCol;
            this.cursorSql = String.format(
                "SELECT * FROM (%s) _c WHERE %s > ? ORDER BY %s FETCH FIRST %d ROWS ONLY",
                baseSql, cursorCol, cursorCol, pageSize
            );
        }

        @Override
        public List<Row> get() {
            List<Map<String, Object>> rows = jdbc.queryForList(cursorSql, lastCursor);
            batchNo++;
            if (!rows.isEmpty()) {
                // Extract cursor value by column name, not position
                Map<String, Object> lastMap = rows.get(rows.size() - 1);
                lastCursor = lastMap.get(cursorCol.toLowerCase());
                log.debug("Cursor batch {}: {} rows, cursor={}", batchNo, rows.size(), lastCursor);
            }
            return rows.stream().map(JdbcExtractor::mapToRow).toList();
        }
    }

    static Row mapToRow(Map<String, Object> map) {
        Map<String, Object> values = new LinkedHashMap<>();
        map.forEach((k, v) -> values.put(k, v));
        return new Row(values);
    }
}
