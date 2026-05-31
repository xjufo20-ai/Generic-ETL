package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinProcessor implements TransformProcessor {
    private static final Pattern HOLDER = Pattern.compile(":([a-zA-Z_][a-zA-Z0-9_]*)");
    private final DataSource dataSource;
    public JoinProcessor(DataSource dataSource) { this.dataSource = dataSource; }
    @Override public Row process(Row row, TransformDef def) { throw new UnsupportedOperationException("Use processSet()"); }
    @Override public boolean isSetProcessor() { return true; }

    @Override
    public List<Row> processSet(List<Row> rows, TransformDef def) {
        if (!(def instanceof TransformDef.JoinDef j)) return rows;
        if (j.getQuery() == null || j.getQuery().isBlank()) return rows;

        String joinType = j.getJoinType() != null ? j.getJoinType().toUpperCase() : "INNER";
        String leftKey = j.getRightKey() != null ? j.getRightKey() : (j.getLeftKey() != null ? j.getLeftKey() : "id");
        int rightKeyIdx = j.getRightKey() != null ? Integer.parseInt(j.getRightKey()) : 1;
        List<String> params = extractParams(j.getQuery());

        Map<String, Set<Object>> paramValues = new LinkedHashMap<>();
        for (String p : params) paramValues.put(p, new LinkedHashSet<>());
        for (Row row : rows) {
            for (String p : params) paramValues.get(p).add(row.get(p));
        }

        String sql = j.getQuery();
        List<Object> bindValues = new ArrayList<>();
        for (String p : params) {
            Set<Object> vals = paramValues.get(p);
            sql = sql.replace(":" + p, String.join(",", Collections.nCopies(vals.size(), "?")));
            bindValues.addAll(vals);
        }

        Map<String, List<Row>> joinResults = new LinkedHashMap<>();
        try (var conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < bindValues.size(); i++) ps.setObject(i + 1, bindValues.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                int colCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Row joined = new Row();
                    for (int i = 1; i <= colCount; i++) joined.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
                    joinResults.computeIfAbsent(String.valueOf(rs.getObject(rightKeyIdx)), k -> new ArrayList<>()).add(joined);
                }
            }
        } catch (Exception e) { throw new RuntimeException("Join failed", e); }

        List<Row> result = new ArrayList<>();
        for (Row leftRow : rows) {
            String key = String.valueOf(leftRow.get(leftKey));
            List<Row> matches = joinResults.getOrDefault(key, List.of());
            if (!matches.isEmpty()) {
                for (Row match : matches) {
                    Row merged = leftRow.copy(); match.getValues().forEach(merged::put); result.add(merged);
                }
            } else if ("LEFT".equals(joinType)) result.add(leftRow);
        }
        return result;
    }

    private List<String> extractParams(String sql) { List<String> p = new ArrayList<>(); Matcher m = HOLDER.matcher(sql); while (m.find()) p.add(m.group(1)); return p; }
}
