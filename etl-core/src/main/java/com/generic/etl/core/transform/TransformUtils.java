package com.generic.etl.core.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared utilities for Camel processors — reduces duplication of
 * header-parsing and body-casting logic across transforms.
 */
public final class TransformUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TransformUtils() {}

    /** Read a JSON header value, supporting both Map and String types. */
    @SuppressWarnings("unchecked")
    public static <T> T readHeader(Exchange exchange, String name, Class<T> type) {
        Object val = exchange.getIn().getHeader(name);
        if (val == null) return null;
        if (type.isInstance(val)) return (T) val;
        if (val instanceof String s) {
            try {
                if (type == Map.class) {
                    return (T) MAPPER.readValue(s, new TypeReference<LinkedHashMap<String, Object>>() {});
                }
                return MAPPER.readValue(s, type);
            } catch (Exception ignored) {}
        }
        return null;
    }

    /** Get body as List<Map>, or return empty list. */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> bodyAsMapList(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        if (body instanceof List<?> list && !list.isEmpty()
                && list.get(0) instanceof Map) {
            return (List<Map<String, Object>>) list;
        }
        return List.of();
    }
}
