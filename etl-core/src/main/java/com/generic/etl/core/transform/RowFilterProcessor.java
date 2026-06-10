package com.generic.etl.core.transform;

import com.generic.etl.core.expression.ExpressionEvaluator;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("rowFilter")
public class RowFilterProcessor extends AbstractTransformProcessor {

    @Override
    protected List<Map<String, Object>> doTransform(List<Map<String, Object>> rows, Exchange exchange) {
        String expr = exchange.getIn().getHeader("filterExpr", String.class);
        if (expr == null || expr.isBlank()) return null;

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (ExpressionEvaluator.evaluateMap(row, expr)) {
                result.add(row);
            }
        }
        return result;
    }
}
