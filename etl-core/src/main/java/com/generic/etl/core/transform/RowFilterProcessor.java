package com.generic.etl.core.transform;

import com.generic.etl.core.expression.ExpressionEvaluator;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("rowFilter")
public class RowFilterProcessor implements Processor {

    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) {
        String expr = exchange.getIn().getHeader("filterExpr", String.class);
        if (expr == null || expr.isBlank()) return;

        Object body = exchange.getIn().getBody();
        if (!(body instanceof List<?> list)) return;

        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> row) {
                if (ExpressionEvaluator.evaluateMap((Map<String, Object>) row, expr)) {
                    result.add((Map<String, Object>) row);
                }
            }
        }
        exchange.getIn().setBody(result);
    }
}
