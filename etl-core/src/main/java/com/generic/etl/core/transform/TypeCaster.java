package com.generic.etl.core.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;
import java.util.*;

@Component("typeCaster")
public class TypeCaster implements Processor {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Map<String, String> casts = readMap(exchange, "casts");
        if (casts == null) return;
        List<Map<String, Object>> rows = exchange.getIn().getBody(List.class);
        if (rows == null) return;
        for (var row : rows) {
            for (var e : casts.entrySet()) {
                row.computeIfPresent(e.getKey(), (k, v) -> cast(v, e.getValue()));
            }
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

    private static Object cast(Object v, String to) {
        if (v == null) return null;
        return switch (to.toUpperCase()) {
            case "STRING" -> v.toString();
            case "LONG" -> v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
            case "DOUBLE","DECIMAL" -> v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
            case "INT","INTEGER" -> v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
            case "BOOLEAN" -> v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
            default -> v;
        };
    }
}
