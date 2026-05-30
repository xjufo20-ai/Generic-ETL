package com.generic.etl.load.persist;

import com.generic.etl.common.model.PersistConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PersistHandler {
    private final DataSource dataSource;

    public PersistHandler(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int persistIfNeeded(List<Row> rows, PersistConfig persist) {
        if (persist == null || !persist.isEnabled()) {
            log.debug("Persist not enabled, skipping");
            return 0;
        }

        if (rows.size() < persist.getThreshold()) {
            log.debug("Row count {} below threshold {}, skipping persist", rows.size(), persist.getThreshold());
            return 0;
        }

        PersistConfig.StorageConfig storage = persist.getStorage();
        if (storage == null || storage.getTable() == null) {
            log.warn("Persist enabled but no storage config, skipping");
            return 0;
        }

        return persistRows(rows, storage);
    }

    private int persistRows(List<Row> rows, PersistConfig.StorageConfig storage) {
        if (rows.isEmpty()) return 0;

        String tableName = storage.getTable();
        List<String> columns = rows.get(0).getValues().keySet().stream().toList();

        String columnList = String.join(", ", columns);
        String placeholderList = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholderList);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int count = 0;
            for (Row row : rows) {
                int idx = 1;
                for (String col : columns) {
                    ps.setObject(idx++, row.get(col));
                }
                ps.addBatch();
                count++;

                if (count % 1000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();
            log.info("Persisted {} rows to table {}", count, tableName);
            return count;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to persist rows to " + tableName, e);
        }
    }
}
