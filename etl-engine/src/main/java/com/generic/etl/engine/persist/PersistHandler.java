package com.generic.etl.engine.persist;

import com.generic.etl.common.model.ConnectionConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;

/**
 * JDBC persist with cross-database upsert support.
 *
 * Supported dialects: PostgreSQL, MySQL, H2.
 * The dialect is auto-detected from the JDBC URL or ConnectionConfig.
 */
@Slf4j
public class PersistHandler {

    private final DataSource dataSource;

    public PersistHandler(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Insert rows using the default DataSource. */
    public int insert(String table, List<Row> rows, List<String> primaryKeys) {
        return insert(table, rows, primaryKeys, null);
    }

    /** Insert rows, optionally using a per-pipeline DB connection. */
    public int insert(String table, List<Row> rows, List<String> primaryKeys, ConnectionConfig connConfig) {
        if (rows.isEmpty()) return 0;
        List<String> cols = rows.get(0).getValues().keySet().stream().toList();
        String url = connConfig != null ? connConfig.getUrl() : resolveDefaultUrl();
        SqlDialect dialect = SqlDialect.detect(url);
        String sql = dialect.buildUpsert(table, cols, primaryKeys);
        if (connConfig != null) {
            return insertWithDriverManager(connConfig, sql, rows, cols, table);
        } else {
            return insertWithDataSource(sql, rows, cols, table);
        }
    }

    /** Camel 4 bean entry point — reads table & primaryKeys from exchange headers. */
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        String table = exchange.getIn().getHeader("persistTable", String.class);
        String pkStr = exchange.getIn().getHeader("persistPrimaryKeys", String.class);
        List<Row> rows = exchange.getIn().getBody(List.class);
        List<String> primaryKeys = pkStr != null && !pkStr.isEmpty()
                ? List.of(pkStr.split(",")) : null;
        insert(table, rows, primaryKeys);
    }

    private String resolveDefaultUrl() {
        try (Connection c = dataSource.getConnection()) {
            return c.getMetaData().getURL();
        } catch (SQLException e) {
            log.warn("Could not resolve default DataSource URL, assuming PostgreSQL");
            return "jdbc:postgresql:";
        }
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

    // ── SQL Dialect strategy ──────────────────────────────────────────

    enum SqlDialect {
        POSTGRESQL {
            @Override
            String buildUpsert(String table, List<String> cols, List<String> primaryKeys) {
                if (primaryKeys == null || primaryKeys.isEmpty()) {
                    return buildSimpleInsert(table, cols);
                }
                String updateSet = cols.stream().filter(c -> !primaryKeys.contains(c))
                        .map(c -> c + " = EXCLUDED." + c).collect(Collectors.joining(", "));
                return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                        table, joinCols(cols), joinPlaceholders(cols),
                        String.join(", ", primaryKeys), updateSet);
            }
        },
        MYSQL {
            @Override
            String buildUpsert(String table, List<String> cols, List<String> primaryKeys) {
                if (primaryKeys == null || primaryKeys.isEmpty()) {
                    return buildSimpleInsert(table, cols);
                }
                String updateSet = cols.stream().filter(c -> !primaryKeys.contains(c))
                        .map(c -> c + " = VALUES(" + c + ")").collect(Collectors.joining(", "));
                return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        table, joinCols(cols), joinPlaceholders(cols), updateSet);
            }
        },
        H2 {
            @Override
            String buildUpsert(String table, List<String> cols, List<String> primaryKeys) {
                if (primaryKeys == null || primaryKeys.isEmpty()) {
                    return buildSimpleInsert(table, cols);
                }
                // H2 supports MERGE INTO
                String keyCondition = primaryKeys.stream()
                        .map(pk -> "t." + pk + " = s." + pk)
                        .collect(Collectors.joining(" AND "));
                String keyCols = String.join(", ", primaryKeys);
                return String.format("MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s)"
                        + " WHEN MATCHED THEN UPDATE SET %s"
                        + " WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                        table, joinPlaceholdersWithAlias(cols, "s"), keyCondition,
                        cols.stream().filter(c -> !primaryKeys.contains(c))
                                .map(c -> c + " = s." + c).collect(Collectors.joining(", ")),
                        joinCols(cols), joinPlaceholdersWithAlias(cols, "s"));
            }
        };

        abstract String buildUpsert(String table, List<String> cols, List<String> primaryKeys);

        static String buildSimpleInsert(String table, List<String> cols) {
            return String.format("INSERT INTO %s (%s) VALUES (%s)",
                    table, joinCols(cols), joinPlaceholders(cols));
        }

        static SqlDialect detect(String url) {
            if (url == null) return POSTGRESQL;
            String lower = url.toLowerCase();
            if (lower.contains("mysql")) return MYSQL;
            if (lower.contains("h2")) return H2;
            return POSTGRESQL; // default
        }
    }

    private static String joinCols(List<String> cols) { return String.join(", ", cols); }
    private static String joinPlaceholders(List<String> cols) {
        return cols.stream().map(c -> "?").collect(Collectors.joining(", "));
    }
    private static String joinPlaceholdersWithAlias(List<String> cols, String alias) {
        return cols.stream().map(c -> "?").collect(Collectors.joining(", "));
    }
}
