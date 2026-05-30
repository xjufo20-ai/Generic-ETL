package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class AggregateProcessor implements TransformProcessor {
    @Override
    public Row process(Row row, TransformDef def) {
        throw new UnsupportedOperationException("Use processSet()");
    }

    @Override public boolean isSetProcessor() { return true; }

    @Override
    public List<Row> processSet(List<Row> rows, TransformDef def) {
        if (!(def instanceof TransformDef.AggregateDef agg)) return rows;
        if (agg.getGroupBy() == null || agg.getGroupBy().isEmpty()) {
            return List.of(computeAggregates(rows, agg.getAggregations()));
        }
        Map<String, List<Row>> groups = rows.stream()
                .collect(Collectors.groupingBy(r -> buildGroupKey(r, agg.getGroupBy()), LinkedHashMap::new, Collectors.toList()));
        List<Row> results = new ArrayList<>();
        for (Map.Entry<String, List<Row>> entry : groups.entrySet()) {
            Row result = computeAggregates(entry.getValue(), agg.getAggregations());
            Row first = entry.getValue().getFirst();
            for (String gb : agg.getGroupBy()) result.put(gb, first.get(gb));
            results.add(result);
        }
        return results;
    }

    private String buildGroupKey(Row row, List<String> groupBy) {
        return groupBy.stream().map(gb -> String.valueOf(row.get(gb))).collect(Collectors.joining("::"));
    }

    private Row computeAggregates(List<Row> rows, List<TransformDef.Aggregation> aggs) {
        Row result = new Row();
        for (TransformDef.Aggregation agg : aggs) {
            Object value = switch (agg.getFunction().toUpperCase()) {
                case "COUNT" -> (long) rows.size();
                case "SUM" -> sum(rows, agg.getField());
                case "AVG" -> avg(rows, agg.getField());
                case "MIN" -> min(rows, agg.getField());
                case "MAX" -> max(rows, agg.getField());
                default -> throw new IllegalArgumentException("Unknown: " + agg.getFunction());
            };
            result.put(agg.getAlias() != null ? agg.getAlias() : agg.getField(), value);
        }
        return result;
    }

    private BigDecimal sum(List<Row> rows, String field) { return rows.stream().map(r -> toBD(r.get(field))).filter(Objects::nonNull).reduce(BigDecimal.ZERO, BigDecimal::add); }
    private BigDecimal avg(List<Row> rows, String field) { List<BigDecimal> vals = rows.stream().map(r -> toBD(r.get(field))).filter(Objects::nonNull).toList(); return vals.isEmpty() ? BigDecimal.ZERO : vals.stream().reduce(BigDecimal.ZERO, BigDecimal::add).divide(BigDecimal.valueOf(vals.size()), 4, java.math.RoundingMode.HALF_UP); }
    private BigDecimal min(List<Row> rows, String field) { return rows.stream().map(r -> toBD(r.get(field))).filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(BigDecimal.ZERO); }
    private BigDecimal max(List<Row> rows, String field) { return rows.stream().map(r -> toBD(r.get(field))).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(BigDecimal.ZERO); }
    private BigDecimal toBD(Object val) { if (val == null) return null; return val instanceof BigDecimal bd ? bd : new BigDecimal(val.toString()); }
}
