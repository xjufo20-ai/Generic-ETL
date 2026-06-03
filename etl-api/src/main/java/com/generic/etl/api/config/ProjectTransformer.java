package com.generic.etl.api.config;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Camel Processor: maps source columns → canonical column names.
 * Referenced in YAML DSL as: bean:projectTransformer
 *
 * Usage in YAML:
 *   - bean:
 *       ref: projectTransformer
 *       parameters:
 *         mappings:
 *           emp_id: id
 *           emp_name: name
 */
@Component("projectTransformer")
public class ProjectTransformer implements Processor {

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        Map<String, String> mappings = exchange.getIn().getHeader("mappings", Map.class);
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
}
