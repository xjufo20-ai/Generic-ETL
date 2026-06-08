package com.generic.etl.core.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("etlAggregator")
public class EtlAggregator implements Processor {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Map<String, Object> params = readParams(exchange);
        if (params == null) return;

        List<String> groupBy = (List<String>) params.getOrDefault("groupBy", List.of());
        List<Map<String, String>> aggDefs = readAggDefs(params);
        if (aggDefs == null) return;

        List<Map<String, Object>> rows = exchange.getIn().getBody(List.class);
        if (rows == null || rows.isEmpty()) return;

        Map<String, List<Map<String, Object>>> groups;
        if (groupBy.isEmpty()) {
            groups = Map.of("__all__", rows);
        } else {
            groups = rows.stream().collect(Collectors.groupingBy(
                r -> groupBy.stream().map(g -> String.valueOf(r.get(g))).collect(Collectors.joining("::")),
                LinkedHashMap::new, Collectors.toList()));
        }

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

    private Map<String, Object> readParams(Exchange exchange) {
        try {
            Object val = exchange.getIn().getHeader("etlAgg");
            if (val instanceof Map m) return m;
            if (val instanceof String s) return mapper.readValue(s, new TypeReference<Map<String, Object>>() {});
        } catch (Exception ignored) {}
        return null;
    }

    private List<Map<String, String>> readAggDefs(Map<String, Object> params) {
        Object defs = params.get("aggregations");
        if (defs instanceof List l) {
            // already deserialized as List<Map> from JSON
            List<Map<String, String>> result = new ArrayList<>();
            for (Object item : l) {
                if (item instanceof Map m) {
                    Map<String, String> agg = new LinkedHashMap<>();
                    m.forEach((k, v) -> agg.put(k.toString(), v != null ? v.toString() : ""));
                    result.add(agg);
                }
            }
            return result;
        }
        return null;
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
