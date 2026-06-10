package com.generic.etl.core.transform;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("etlAggregator")
public class EtlAggregator extends AbstractTransformProcessor {

    @Override
    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> doTransform(List<Map<String, Object>> rows, Exchange exchange) {
        Map<String, Object> params = TransformUtils.readHeader(exchange, "etlAgg", Map.class);
        if (params == null) return null;

        List<String> groupBy = (List<String>) params.getOrDefault("groupBy", List.of());
        List<Map<String, String>> aggDefs = resolveAggDefs(params.get("aggregations"));
        if (aggDefs == null) return null;

        // Group
        Map<String, List<Map<String, Object>>> groups;
        if (groupBy.isEmpty()) {
            groups = Map.of("__all__", rows);
        } else {
            groups = rows.stream().collect(Collectors.groupingBy(
                r -> groupBy.stream()
                        .map(g -> Objects.toString(r.get(g), ""))
                        .collect(Collectors.joining("::")),
                LinkedHashMap::new, Collectors.toList()));
        }

        // Aggregate
        List<Map<String, Object>> result = new ArrayList<>();
        for (var entry : groups.entrySet()) {
            Map<String, Object> agg = new LinkedHashMap<>();
            Map<String, Object> first = entry.getValue().get(0);
            for (String g : groupBy) agg.put(g, first.get(g));

            for (var def : aggDefs) {
                String field = def.get("field");
                String fn = def.get("function").toUpperCase();
                String alias = def.getOrDefault("alias", field);
                agg.put(alias, compute(fn, entry.getValue(), field));
            }
            result.add(agg);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, String>> resolveAggDefs(Object defs) {
        if (!(defs instanceof List<?> list)) return null;
        List<Map<String, String>> result = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> m) {
                Map<String, String> agg = new LinkedHashMap<>();
                m.forEach((k, v) -> agg.put(k.toString(), v != null ? v.toString() : ""));
                result.add(agg);
            }
        }
        return result;
    }

    private static Number compute(String fn, List<Map<String, Object>> rows, String field) {
        return switch (fn) {
            case "COUNT" -> (long) rows.size();
            case "SUM"   -> rows.stream().mapToDouble(r -> toDouble(r.get(field))).sum();
            case "AVG"   -> rows.stream().mapToDouble(r -> toDouble(r.get(field))).average().orElse(0);
            case "MIN"   -> rows.stream().mapToDouble(r -> toDouble(r.get(field))).min().orElse(0);
            case "MAX"   -> rows.stream().mapToDouble(r -> toDouble(r.get(field))).max().orElse(0);
            default      -> 0L;
        };
    }

    private static double toDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        if (v != null) {
            try { return Double.parseDouble(v.toString()); }
            catch (NumberFormatException ignored) {}
        }
        return 0;
    }
}
