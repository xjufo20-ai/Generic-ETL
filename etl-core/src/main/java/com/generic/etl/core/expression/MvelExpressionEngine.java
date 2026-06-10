package com.generic.etl.core.expression;

import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MVEL-based expression engine with compiled expression cache.
 */
public class MvelExpressionEngine implements ExpressionEngine {

    private final Map<String, Serializable> cache = new ConcurrentHashMap<>();

    @Override
    public String name() { return "MVEL"; }

    @Override
    public boolean evaluate(Map<String, Object> context, String expression) {
        if (expression == null || expression.isBlank()) return true;
        try {
            Serializable compiled = cache.computeIfAbsent(expression, MVEL::compileExpression);
            Object result = MVEL.executeExpression(compiled, context);
            return result instanceof Boolean b && b;
        } catch (Exception e) {
            throw new RuntimeException("MVEL evaluation failed: " + expression, e);
        }
    }
}
