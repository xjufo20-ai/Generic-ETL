package com.generic.etl.api.config;

import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.expression.ExpressionEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;

import java.util.*;

/** Maps TransformDef → Camel EIP step on a RouteBuilder. */
@Slf4j
public class TransformEipMapper {

    /** Apply a single transform as a Camel EIP step. */
    @SuppressWarnings("unchecked")
    public static void apply(RouteBuilder rb, TransformDef def) {
        if (def instanceof TransformDef.FilterDef f) {
            String expr = f.getExpression();
            rb.filter(e -> {
                Map<String, Object> row = e.getIn().getBody(Map.class);
                return row != null && ExpressionEvaluator.evaluateMap(row, expr);
            });
        } else if (def instanceof TransformDef.RenameDef r) {
            rb.process(e -> {
                Map<String, Object> row = e.getIn().getBody(Map.class);
                if (row != null && r.getMappings() != null) {
                    for (var m : r.getMappings()) {
                        Object v = row.remove(m.getFrom());
                        row.put(m.getTo(), v);
                    }
                }
            });
        } else if (def instanceof TransformDef.TypeCastDef c) {
            rb.process(e -> {
                Map<String, Object> row = e.getIn().getBody(Map.class);
                if (row != null && c.getMappings() != null) {
                    for (var m : c.getMappings())
                        row.computeIfPresent(m.getField(), (k, v) -> cast(v, m.getToType()));
                }
            });
        } else if (def instanceof TransformDef.AggregateDef a) {
            List<String> gb = a.getGroupBy() != null ? a.getGroupBy() : List.of();
            List<TransformDef.Aggregation> aggs = a.getAggregations() != null ? a.getAggregations() : List.of();
            rb.aggregate(rb.header("pipelineName"), new GroupedBodyAggregationStrategy())
                .completionTimeout(5000);
            rb.process(e -> {
                List<Map<String, Object>> group = e.getIn().getBody(List.class);
                e.getIn().setBody(List.of(aggregateRows(group, aggs, gb)));
            });
        } else if (def instanceof TransformDef.JoinDef j) {
            rb.enrich("jdbc:etlDataSource?query=" + j.getQuery() + "&outputType=SelectOne",
                (oldEx, newEx) -> {
                    Map<String, Object> row = oldEx.getIn().getBody(Map.class);
                    Map<String, Object> enriched = newEx.getIn().getBody(Map.class);
                    if (row != null && enriched != null) row.putAll(enriched);
                    return oldEx;
                });
        } else if (def instanceof TransformDef.SplitDef) {
            rb.split(rb.body());
        } else {
            log.warn("Unknown transform type: {}", def.getType());
        }
    }

    // ── Aggregation helpers ───────────────────────────────────

    static Map<String, Object> aggregateRows(List<Map<String, Object>> rows,
                                               List<TransformDef.Aggregation> aggs,
                                               List<String> groupBy) {
        Map<String, Object> r = new LinkedHashMap<>();
        if (!groupBy.isEmpty() && !rows.isEmpty())
            for (String gb : groupBy) r.put(gb, rows.get(0).get(gb));
        for (var a : aggs) {
            String fn = a.getFunction().toUpperCase(), f = a.getField();
            String alias = a.getAlias() != null ? a.getAlias() : f;
            r.put(alias, compute(fn, rows, f));
        }
        return r;
    }

    private static Object compute(String fn, List<Map<String, Object>> rows, String field) {
        return switch (fn) {
            case "COUNT" -> (long) rows.size();
            case "SUM" -> rows.stream().mapToDouble(x -> toD(x.get(field))).sum();
            case "AVG" -> rows.stream().mapToDouble(x -> toD(x.get(field))).average().orElse(0);
            case "MIN" -> rows.stream().mapToDouble(x -> toD(x.get(field))).min().orElse(0);
            case "MAX" -> rows.stream().mapToDouble(x -> toD(x.get(field))).max().orElse(0);
            default -> 0L;
        };
    }

    private static double toD(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (NumberFormatException e) { return 0; }
    }

    static Object cast(Object v, String to) {
        if (v == null) return null;
        return switch (to.toUpperCase()) {
            case "STRING" -> v.toString();
            case "LONG" -> v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
            case "DOUBLE", "DECIMAL" -> v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
            case "INT", "INTEGER" -> v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
            case "BOOLEAN" -> v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
            default -> v;
        };
    }
}
