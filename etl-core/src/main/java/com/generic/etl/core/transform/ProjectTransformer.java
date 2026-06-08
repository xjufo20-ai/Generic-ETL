package com.generic.etl.core.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("projectTransformer")
public class ProjectTransformer implements Processor {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Map<String, String> mappings = readMap(exchange, "mappings");
        if (mappings == null) return;

        Object body = exchange.getIn().getBody();
        if (body instanceof List<?> list) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?,?> row) {
                    Map<String, Object> projected = new LinkedHashMap<>();
                    for (var e : mappings.entrySet()) {
                        projected.put(e.getValue(), ((Map<String,Object>)row).get(e.getKey()));
                    }
                    result.add(projected);
                }
            }
            exchange.getIn().setBody(result);
        }
    }

    private Map<String, String> readMap(Exchange exchange, String name) {
        try {
            Object val = exchange.getIn().getHeader(name);
            if (val instanceof Map m) return (Map<String, String>) m;
            if (val instanceof String s) return mapper.readValue(s, new TypeReference<Map<String, String>>() {});
        } catch (Exception ignored) {}
        return null;
    }
}
