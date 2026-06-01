package com.generic.etl.extract.adapter.impl;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.extract.adapter.DataSourceManager;
import com.generic.etl.extract.adapter.Extractor;
import com.generic.etl.extract.adapter.WatermarkStore;
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
    private final WatermarkStore watermarkStore;

    public JdbcExtractor(DataSourceManager dsManager, WatermarkStore watermarkStore) {
        this.dsManager = dsManager;
        this.watermarkStore = watermarkStore;
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

        String query = ds.getQuery();

        // Apply watermark for incremental extraction
        if (config.getWatermark() != null) {
            String col = config.getWatermark().getColumn();
            String lastVal = watermarkStore.get(config.getPipeline().getName());
            if (lastVal != null) {
                query = query + " AND " + col + " > '" + lastVal + "'";
                log.debug("Incremental extract: watermark={}", lastVal);
            } else if (config.getWatermark().getInitial() != null) {
                query = query + " AND " + col + " >= '" + config.getWatermark().getInitial() + "'";
            }
        }

        if (ds.getCursor() != null) {
            return extractWithCursor(jdbc, query, ds, config);
        }
        List<Map<String, Object>> rows = jdbc.queryForList(query);
        updateWatermark(rows, config);
        log.debug("Extracted {} rows", rows.size());
        return rows.stream().map(JdbcExtractor::mapToRow);
    }

    private Stream<Row> extractWithCursor(JdbcTemplate jdbc, String baseSql, DataSourceConfig.JdbcDataSource ds, PipelineConfig config) {
        String col = ds.getCursor().getColumn();
        int pageSize = ds.getCursor().getPageSize();
        String cursorSql = String.format("SELECT * FROM (%s) _c WHERE %s > ? ORDER BY %s FETCH FIRST %d ROWS ONLY",
                baseSql, col, col, pageSize);

        return Stream.generate(new CursorSupplier(jdbc, cursorSql, col, pageSize, config, watermarkStore, this))
                .takeWhile(list -> !list.isEmpty())
                .flatMap(List::stream);
    }

    void updateWatermark(List<Map<String, Object>> rows, PipelineConfig config) {
        if (config.getWatermark() != null && !rows.isEmpty()) {
            String col = config.getWatermark().getColumn();
            Object lastVal = rows.get(rows.size() - 1).get(col);
            if (lastVal != null) {
                watermarkStore.set(config.getPipeline().getName(), lastVal.toString());
            }
        }
    }

    private static class CursorSupplier implements java.util.function.Supplier<List<Row>> {
        private final JdbcTemplate jdbc;
        private final String cursorSql;
        private final String cursorCol;
        private final PipelineConfig config;
        private final WatermarkStore watermarkStore;
        private final JdbcExtractor extractor;
        private Object lastCursor;
        private int batchNo;

        CursorSupplier(JdbcTemplate jdbc, String cursorSql, String cursorCol, int pageSize,
                       PipelineConfig config, WatermarkStore watermarkStore, JdbcExtractor extractor) {
            this.jdbc = jdbc;
            this.cursorSql = cursorSql;
            this.cursorCol = cursorCol;
            this.config = config;
            this.watermarkStore = watermarkStore;
            this.extractor = extractor;
        }

        @Override
        public List<Row> get() {
            List<Map<String, Object>> rows = jdbc.queryForList(cursorSql, lastCursor);
            batchNo++;
            if (!rows.isEmpty()) {
                Map<String, Object> lastMap = rows.get(rows.size() - 1);
                lastCursor = lastMap.get(cursorCol);
                extractor.updateWatermark(rows, config);
                log.debug("Cursor batch {}: {} rows, cursor={}", batchNo, rows.size(), lastCursor);
            }
            return rows.stream().map(JdbcExtractor::mapToRow).toList();
        }
    }

    static Row mapToRow(Map<String, Object> map) {
        Map<String, Object> values = new LinkedHashMap<>();
        map.forEach(values::put);
        return new Row(values);
    }
}
