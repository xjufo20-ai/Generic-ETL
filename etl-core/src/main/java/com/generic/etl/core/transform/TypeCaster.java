package com.generic.etl.core.transform;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("typeCaster")
public class TypeCaster extends AbstractTransformProcessor {

    @Override
    protected List<Map<String, Object>> doTransform(List<Map<String, Object>> rows, Exchange exchange) {
        Map<String, String> casts = TransformUtils.readHeader(exchange, "casts", Map.class);
        if (casts == null) return null;

        for (var row : rows) {
            for (var e : casts.entrySet()) {
                row.computeIfPresent(e.getKey(), (k, v) -> cast(v, e.getValue()));
            }
        }
        return rows;
    }

    private static Object cast(Object v, String to) {
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
