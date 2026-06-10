package com.generic.etl.engine;

import com.generic.etl.common.model.Row;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Normalizes the body after a Camel sql: producer call into List&lt;Map&lt;String, Object&gt;&gt;.
 * Also lowercases all map keys to handle cross-database column name case differences
 * (H2 returns uppercase, PostgreSQL lowercase, MySQL varies by OS).
 */
public class SqlResultNormalizer implements Processor {

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        if (body == null) return;

        List<Map<String, Object>> normalized;

        if (body instanceof List<?> list) {
            if (list.isEmpty()) {
                exchange.getIn().setBody(List.of());
                return;
            }
            Object first = list.get(0);

            if (first instanceof Map) {
                normalized = list.stream()
                        .map(m -> lowerCaseKeys((Map<String, Object>) m))
                        .collect(Collectors.toList());
            } else if (first instanceof Row r) {
                normalized = list.stream()
                        .map(item -> lowerCaseKeys(((Row) item).getValues()))
                        .collect(Collectors.toList());
            } else if (first instanceof List) {
                normalized = new ArrayList<>();
                for (Object row : list) {
                    Map<String, Object> mapRow = new LinkedHashMap<>();
                    int colIdx = 0;
                    for (Object val : (List<?>) row) {
                        mapRow.put("col" + colIdx++, val);
                    }
                    normalized.add(mapRow);
                }
            } else {
                normalized = new ArrayList<>();
                for (Object item : list) {
                    Map<String, Object> mapRow = new LinkedHashMap<>();
                    mapRow.put("_value", item);
                    normalized.add(mapRow);
                }
            }
        } else if (body instanceof Map<?, ?> m) {
            normalized = List.of(lowerCaseKeys((Map<String, Object>) m));
        } else {
            Map<String, Object> mapRow = new LinkedHashMap<>();
            mapRow.put("_value", body);
            normalized = List.of(mapRow);
        }

        exchange.getIn().setBody(normalized);
    }

    /** Copy a map with all keys lowercased. */
    private static Map<String, Object> lowerCaseKeys(Map<String, Object> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((k, v) -> result.put(k.toLowerCase(Locale.ROOT), v));
        return result;
    }
}
