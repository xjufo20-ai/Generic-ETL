package com.generic.etl.core.expression;

import com.generic.etl.common.model.Row;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExpressionEvaluator {
    private static final Map<String, Serializable> CACHE = new ConcurrentHashMap<>();

    public static boolean evaluate(Row row, String expression) {
        if (expression == null || expression.isBlank()) return true;
        try {
            Serializable compiled = CACHE.computeIfAbsent(expression, MVEL::compileExpression);
            Object result = MVEL.executeExpression(compiled, row.getValues());
            return result instanceof Boolean b && b;
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate: " + expression, e);
        }
    }

    public static Object evaluateValue(Row row, String expression) {
        if (expression == null || expression.isBlank()) return null;
        try {
            Serializable compiled = CACHE.computeIfAbsent(expression, MVEL::compileExpression);
            return MVEL.executeExpression(compiled, row.getValues());
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate: " + expression, e);
        }
    }
}
