package com.generic.etl.load.persist;

import com.generic.etl.common.model.ConnectionConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC persist — insert with optional upsert.
 *
 * Two modes:
 *   1) Default DataSource (Spring-managed) — for most pipelines.
 *   2) Per-pipeline ConnectionConfig — creates temporary connection via DriverManager.
 */
@Slf4j
public class PersistHandler {
    private final DataSource dataSource;

    public PersistHandler(DataSource dataSource) { this.dataSource = dataSource; }

    /** Insert rows using the default DataSource. */
    public int insert(String table, List<Row> rows, List<String> primaryKeys) {
        return insert(table, rows, primaryKeys, null);
    }

    /** Insert rows, optionally using a per-pipeline DB connection. */
    public int insert(String table, List<Row> rows, List<String> primaryKeys, ConnectionConfig connConfig) {
        if (rows.isEmpty()) return 0;
        List<String> cols = rows.get(0).getValues().keySet().stream().toList();
        String sql = buildSql(table, cols, primaryKeys);
        if (connConfig != null) {
            return insertWithDriverManager(connConfig, sql, rows, cols, table);
        } else {
            return insertWithDataSource(sql, rows, cols, table);
        }
    }

    private String buildSql(String table, List<String> cols, List<String> primaryKeys) {
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String updateSet = cols.stream().filter(c -> !primaryKeys.contains(c))
                    .map(c -> c + " = EXCLUDED." + c).collect(Collectors.joining(", "));
            return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                    table, joinCols(cols), joinPlaceholders(cols), String.join(", ", primaryKeys), updateSet);
        }
        return String.format("INSERT INTO %s (%s) VALUES (%s)", table, joinCols(cols), joinPlaceholders(cols));
    }

    private int insertWithDataSource(String sql, List<Row> rows, List<String> cols, String table) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            int count = executeBatch(ps, rows, cols);
            c.commit();
            log.info("Persisted {} rows to {}", count, table);
            return count;
        } catch (SQLException e) {
            throw new RuntimeException("Persist failed: " + table, e);
        }
    }

    private int insertWithDriverManager(ConnectionConfig conn, String sql, List<Row> rows,
                                         List<String> cols, String table) {
        try (Connection c = DriverManager.getConnection(conn.getUrl(), conn.getUsername(), conn.getPassword());
             PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            int count = executeBatch(ps, rows, cols);
            c.commit();
            log.info("Persisted {} rows to {} @ {}", count, table, conn.getUrl());
            return count;
        } catch (SQLException e) {
            throw new RuntimeException("Persist failed: " + table + " @ " + conn.getUrl(), e);
        }
    }

    private int executeBatch(PreparedStatement ps, List<Row> rows, List<String> cols) throws SQLException {
        int count = 0;
        for (Row row : rows) {
            int idx = 1;
            for (String col : cols) ps.setObject(idx++, row.get(col));
            ps.addBatch();
            if (++count % 1000 == 0) ps.executeBatch();
        }
        ps.executeBatch();
        return count;
    }

    private static String joinCols(List<String> cols) { return String.join(", ", cols); }
    private static String joinPlaceholders(List<String> cols) {
        return cols.stream().map(c -> "?").collect(Collectors.joining(", "));
    }
}
