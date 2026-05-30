package com.generic.etl.transform.join;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class JoinProcessor implements TransformProcessor {

    private final DataSource dataSource;

    public JoinProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Row process(Row row, TransformDef def) {
        throw new UnsupportedOperationException("Join is a set-level transform, use processSet()");
    }

    @Override
    public boolean isSetProcessor() {
        return true;
    }

    @Override
    public List<Row> processSet(List<Row> rows, TransformDef def) {
        if (def.getQuery() == null || def.getQuery().isBlank()) {
            return rows; // No join, pass through
        }

        List<Row> result = new ArrayList<>();
        String joinSql = def.getQuery();
        String joinType = def.getJoinType() != null ? def.getJoinType().toUpperCase() : "INNER";

        try (var conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(joinSql)) {

            // For each row, execute the join query
            for (Row leftRow : rows) {
                // Replace :field placeholders in the join query with row values
                String resolvedSql = resolvePlaceholders(joinSql, leftRow);
                try (PreparedStatement joinPs = conn.prepareStatement(resolvedSql);
                     ResultSet rs = joinPs.executeQuery()) {

                    int colCount = rs.getMetaData().getColumnCount();
                    boolean matched = false;

                    while (rs.next()) {
                        Row joinedRow = leftRow.copy();
                        for (int i = 1; i <= colCount; i++) {
                            String colName = rs.getMetaData().getColumnLabel(i);
                            Object val = rs.getObject(i);
                            joinedRow.put(colName, val);
                        }
                        result.add(joinedRow);
                        matched = true;
                    }

                    if (!matched && joinType.equals("LEFT")) {
                        result.add(leftRow);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Join execution failed", e);
        }

        return result;
    }

    private String resolvePlaceholders(String sql, Row row) {
        for (Map.Entry<String, Object> entry : row.getValues().entrySet()) {
            String placeholder = ":" + entry.getKey();
            Object val = entry.getValue();
            if (val instanceof String || val instanceof java.time.temporal.Temporal) {
                sql = sql.replace(placeholder, "'" + val.toString().replace("'", "''") + "'");
            } else {
                sql = sql.replace(placeholder, val != null ? val.toString() : "NULL");
            }
        }
        return sql;
    }
}
