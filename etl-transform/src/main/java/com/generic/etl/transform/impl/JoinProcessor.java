package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class JoinProcessor implements TransformProcessor {
    private final DataSource dataSource;

    public JoinProcessor(DataSource dataSource) { this.dataSource = dataSource; }

    @Override public Row process(Row row, TransformDef def) { throw new UnsupportedOperationException("Use processSet()"); }
    @Override public boolean isSetProcessor() { return true; }

    @Override
    public List<Row> processSet(List<Row> rows, TransformDef def) {
        if (!(def instanceof TransformDef.JoinDef j)) return rows;
        if (j.getQuery() == null || j.getQuery().isBlank()) return rows;

        List<Row> result = new ArrayList<>();
        String joinType = j.getJoinType() != null ? j.getJoinType().toUpperCase() : "INNER";

        try (var conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(j.getQuery())) {
            for (Row leftRow : rows) {
                String resolvedSql = resolvePlaceholders(j.getQuery(), leftRow);
                try (PreparedStatement jps = conn.prepareStatement(resolvedSql);
                     ResultSet rs = jps.executeQuery()) {
                    int colCount = rs.getMetaData().getColumnCount();
                    boolean matched = false;
                    while (rs.next()) {
                        Row joined = leftRow.copy();
                        for (int i = 1; i <= colCount; i++) joined.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
                        result.add(joined);
                        matched = true;
                    }
                    if (!matched && "LEFT".equals(joinType)) result.add(leftRow);
                }
            }
        } catch (Exception e) { throw new RuntimeException("Join failed", e); }
        return result;
    }

    private String resolvePlaceholders(String sql, Row row) {
        for (Map.Entry<String, Object> e : row.getValues().entrySet()) {
            String p = ":" + e.getKey();
            Object v = e.getValue();
            sql = sql.replace(p, (v instanceof String || v instanceof java.time.temporal.Temporal) ? "'" + v.toString().replace("'", "''") + "'" : v != null ? v.toString() : "NULL");
        }
        return sql;
    }
}
