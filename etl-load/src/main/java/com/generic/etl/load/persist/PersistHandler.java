package com.generic.etl.load.persist;

import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/** Simple JDBC persist — insert with optional upsert. Called by LoadRouter. */
@Slf4j
public class PersistHandler {
    private final DataSource dataSource;

    public PersistHandler(DataSource dataSource) { this.dataSource = dataSource; }

    /** Insert rows into a table. For upsert, pass primary key columns. */
    public int insert(String table, List<Row> rows, List<String> primaryKeys) {
        if (rows.isEmpty()) return 0;
        List<String> cols = rows.get(0).getValues().keySet().stream().toList();

        String sql;
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String updateSet = cols.stream().filter(c -> !primaryKeys.contains(c))
                    .map(c -> c + " = EXCLUDED." + c).collect(Collectors.joining(", "));
            sql = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                    table, joinCols(cols), joinPlaceholders(cols), String.join(", ", primaryKeys), updateSet);
        } else {
            sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table, joinCols(cols), joinPlaceholders(cols));
        }

        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            int count = 0;
            for (Row row : rows) {
                int idx = 1;
                for (String col : cols) ps.setObject(idx++, row.get(col));
                ps.addBatch();
                if (++count % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch(); c.commit();
            log.info("Persisted {} rows to {}", count, table);
            return count;
        } catch (SQLException e) {
            throw new RuntimeException("Persist failed: " + table, e);
        }
    }

    private static String joinCols(List<String> cols) { return String.join(", ", cols); }
    private static String joinPlaceholders(List<String> cols) {
        return cols.stream().map(c -> "?").collect(Collectors.joining(", "));
    }
}
