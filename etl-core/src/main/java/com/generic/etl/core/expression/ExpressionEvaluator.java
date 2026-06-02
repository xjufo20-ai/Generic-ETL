package com.generic.etl.core.expression;

import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExpressionEvaluator {
    private static final Map<String, Serializable> CACHE = new ConcurrentHashMap<>();

    /** Evaluate a boolean expression against a Map (Camel path). */
    public static boolean evaluateMap(Map<String, Object> row, String expression) {
        if (expression == null || expression.isBlank()) return true;
        try {
            Serializable compiled = CACHE.computeIfAbsent(expression, MVEL::compileExpression);
            Object result = MVEL.executeExpression(compiled, row);
            return result instanceof Boolean b && b;
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate: " + expression, e);
        }
    }

    /** Evaluate a boolean expression against a Row (legacy, kept for backward compat). */
    @Deprecated
    public static boolean evaluate(com.generic.etl.common.model.Row row, String expression) {
        return evaluateMap(row.getValues(), expression);
    }
}
