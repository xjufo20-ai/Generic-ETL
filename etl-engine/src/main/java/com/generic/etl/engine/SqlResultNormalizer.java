package com.generic.etl.engine;

import com.generic.etl.common.model.Row;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Normalizes the body after a Camel sql: producer call into List&lt;Map&lt;String, Object&gt;&gt;.
 * Camel's sql: component may return various formats depending on configuration and version;
 * this processor guarantees a uniform format for downstream transform processors.
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
                // Already List<Map> — just ensure mutable LinkedHashMap
                normalized = list.stream()
                        .map(m -> new LinkedHashMap<>((Map<String, Object>) m))
                        .collect(Collectors.toList());
            } else if (first instanceof Row r) {
                normalized = list.stream()
                        .map(item -> new LinkedHashMap<>(((Row) item).getValues()))
                        .collect(Collectors.toList());
            } else if (first instanceof List) {
                // Camel StreamList mode: List<List<Object>>
                normalized = new ArrayList<>();
                int rowIdx = 0;
                for (Object row : list) {
                    Map<String, Object> mapRow = new LinkedHashMap<>();
                    int colIdx = 0;
                    for (Object val : (List<?>) row) {
                        mapRow.put("col" + colIdx++, val);
                    }
                    normalized.add(mapRow);
                    rowIdx++;
                }
            } else {
                // Single-column values: each element becomes {"_value": element}
                normalized = new ArrayList<>();
                for (Object item : list) {
                    Map<String, Object> mapRow = new LinkedHashMap<>();
                    mapRow.put("_value", item);
                    normalized.add(mapRow);
                }
            }
        } else if (body instanceof Map<?, ?> m) {
            normalized = List.of(new LinkedHashMap<>((Map<String, Object>) m));
        } else {
            // Wrap scalar value
            Map<String, Object> mapRow = new LinkedHashMap<>();
            mapRow.put("_value", body);
            normalized = List.of(mapRow);
        }

        exchange.getIn().setBody(normalized);
    }
}
