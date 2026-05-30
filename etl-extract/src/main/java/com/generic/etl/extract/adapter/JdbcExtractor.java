package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JdbcExtractor implements Extractor {
    private static final Logger log = LoggerFactory.getLogger(JdbcExtractor.class);
    private final DataSourceManager dsManager;

    public JdbcExtractor(DataSourceManager dsManager) {
        this.dsManager = dsManager;
    }

    @Override
    public boolean supports(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();
        return ds instanceof DataSourceConfig.JdbcDataSource;
    }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig.JdbcDataSource ds = (DataSourceConfig.JdbcDataSource) config.getDatasource();
        if (ds.getCursor() != null) {
            return extractWithCursor(ds);
        }
        return extractAll(ds);
    }

    private Stream<Row> extractAll(DataSourceConfig.JdbcDataSource ds) {
        Connection conn = getConnection(ds);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(ds.getQuery());
            return resultSetToStream(rs, () -> closeQuietly(rs, stmt, conn));
        } catch (Exception e) {
            closeQuietly(conn);
            throw new RuntimeException("JDBC extraction failed", e);
        }
    }

    private Stream<Row> extractWithCursor(DataSourceConfig.JdbcDataSource ds) {
        String cursorCol = ds.getCursor().getColumn();
        int pageSize = ds.getCursor().getPageSize();
        String baseQuery = ds.getQuery();

        String cursorQuery = buildCursorQuery(baseQuery, cursorCol, pageSize);

        CursorSpliterator spliterator = new CursorSpliterator(ds, cursorQuery, cursorCol, pageSize);
        return StreamSupport.stream(spliterator, false).onClose(spliterator::closeResources);
    }

    private class CursorSpliterator extends Spliterators.AbstractSpliterator<Row> {
        private final DataSourceConfig.JdbcDataSource ds;
        private final String cursorQuery;
        private final String cursorCol;
        private final int pageSize;
        private Object lastCursor = null;
        private boolean exhausted = false;
        private Connection conn;
        private PreparedStatement ps;
        private ResultSet rs;
        private int batchCount = 0;

        CursorSpliterator(DataSourceConfig.JdbcDataSource ds, String cursorQuery, String cursorCol, int pageSize) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            this.ds = ds;
            this.cursorQuery = cursorQuery;
            this.cursorCol = cursorCol;
            this.pageSize = pageSize;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            if (exhausted) return false;

            try {
                if (rs == null || !rs.next()) {
                    closeResources();

                    conn = dsManager.getConnection(ds.getConnection());
                    ps = conn.prepareStatement(cursorQuery);
                    ps.setObject(1, lastCursor);
                    ps.setInt(2, pageSize);
                    rs = ps.executeQuery();

                    batchCount++;
                    log.debug("Cursor batch {}: lastCursor={}, pageSize={}", batchCount, lastCursor, pageSize);

                    if (!rs.next()) {
                        exhausted = true;
                        closeResources();
                        return false;
                    }
                }

                Row row = mapRow(rs);
                lastCursor = row.get(cursorCol);
                action.accept(row);
                return true;
            } catch (Exception e) {
                closeResources();
                throw new RuntimeException("Cursor extraction failed at batch " + batchCount, e);
            }
        }

        void closeResources() {
            closeQuietly(rs);
            closeQuietly(ps);
            closeQuietly(conn);
            rs = null;
            ps = null;
            conn = null;
        }
    }

    private String buildCursorQuery(String baseQuery, String cursorCol, int pageSize) {
        return String.format(
            "SELECT * FROM (%s) _cursor WHERE %s > ? ORDER BY %s FETCH FIRST ? ROWS ONLY",
            baseQuery, cursorCol, cursorCol
        );
    }

    private Connection getConnection(DataSourceConfig.JdbcDataSource ds) {
        return dsManager.getConnection(ds.getConnection());
    }

    private Stream<Row> resultSetToStream(ResultSet rs, Runnable onClose) {
        Spliterator<Row> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE,
                Spliterator.ORDERED | Spliterator.NONNULL) {
            @Override
            public boolean tryAdvance(Consumer<? super Row> action) {
                try {
                    if (rs.next()) {
                        action.accept(mapRow(rs));
                        return true;
                    }
                    return false;
                } catch (SQLException e) {
                    throw new RuntimeException("ResultSet iteration failed", e);
                }
            }
        };
        return StreamSupport.stream(spliterator, false).onClose(onClose);
    }

    private Row mapRow(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            values.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return new Row(values);
    }

    private void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable c : closeables) {
            if (c != null) {
                try { c.close(); } catch (Exception ignored) {}
            }
        }
    }
}
