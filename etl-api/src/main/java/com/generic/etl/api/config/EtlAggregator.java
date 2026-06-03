package com.generic.etl.api.config;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Camel Processor: groupBy + aggregate (SUM/COUNT/AVG/MIN/MAX).
 * Referenced in YAML DSL as: bean:etlAggregator
 *
 * Usage in YAML:
 *   - bean:
 *       ref: etlAggregator
 *       parameters:
 *         groupBy: dept
 *         aggregations:
 *           - {field: salary, function: SUM, alias: total_salary}
 *           - {field: id, function: COUNT, alias: employee_count}
 */
@Component("etlAggregator")
public class EtlAggregator implements Processor {

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Map<String, Object> params = exchange.getIn().getHeader("parameters", Map.class);
        if (params == null) return;

        List<String> groupBy = (List<String>) params.getOrDefault("groupBy", List.of());
        List<Map<String, String>> aggDefs = (List<Map<String, String>>) params.get("aggregations");
        if (aggDefs == null) return;

        List<Map<String, Object>> rows = exchange.getIn().getBody(List.class);
        if (rows == null || rows.isEmpty()) return;

        // Group
        Map<String, List<Map<String, Object>>> groups;
        if (groupBy.isEmpty()) {
            groups = Map.of("__all__", rows);
        } else {
            groups = rows.stream().collect(Collectors.groupingBy(
                r -> groupBy.stream().map(g -> String.valueOf(r.get(g))).collect(Collectors.joining("::")),
                LinkedHashMap::new, Collectors.toList()));
        }

        // Aggregate each group
        List<Map<String, Object>> result = new ArrayList<>();
        for (var entry : groups.entrySet()) {
            Map<String, Object> agg = new LinkedHashMap<>();
            if (!groupBy.isEmpty()) {
                Map<String, Object> first = entry.getValue().get(0);
                for (String g : groupBy) agg.put(g, first.get(g));
            }
            for (var def : aggDefs) {
                String field = def.get("field");
                String fn = def.get("function").toUpperCase();
                String alias = def.getOrDefault("alias", field);
                agg.put(alias, compute(fn, entry.getValue(), field));
            }
            result.add(agg);
        }
        exchange.getIn().setBody(result);
    }

    private static Object compute(String fn, List<Map<String, Object>> rows, String field) {
        return switch (fn) {
            case "COUNT" -> (long) rows.size();
            case "SUM" -> rows.stream().mapToDouble(r -> toD(r.get(field))).sum();
            case "AVG" -> rows.stream().mapToDouble(r -> toD(r.get(field))).average().orElse(0);
            case "MIN" -> rows.stream().mapToDouble(r -> toD(r.get(field))).min().orElse(0);
            case "MAX" -> rows.stream().mapToDouble(r -> toD(r.get(field))).max().orElse(0);
            default -> 0L;
        };
    }

    private static double toD(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (NumberFormatException e) { return 0; }
    }
}
