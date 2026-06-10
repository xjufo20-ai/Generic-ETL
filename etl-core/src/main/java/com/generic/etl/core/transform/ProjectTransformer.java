package com.generic.etl.core.transform;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("projectTransformer")
public class ProjectTransformer extends AbstractTransformProcessor {

    @Override
    protected List<Map<String, Object>> doTransform(List<Map<String, Object>> rows, Exchange exchange) {
        Map<String, String> mappings = TransformUtils.readHeader(exchange, "mappings", Map.class);
        if (mappings == null || mappings.isEmpty()) return null;

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> projected = new LinkedHashMap<>();
            for (var entry : mappings.entrySet()) {
                projected.put(entry.getValue(), row.get(entry.getKey()));
            }
            result.add(projected);
        }
        return result;
    }
}
