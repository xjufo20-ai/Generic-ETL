package com.generic.etl.core.expression;

import java.util.Map;

/**
 * Facade for expression evaluation. Delegates to a pluggable {@link ExpressionEngine}.
 * Default engine is MVEL. Call {@link #setEngine(ExpressionEngine)} to swap.
 */
public final class ExpressionEvaluator {

    private static volatile ExpressionEngine engine = new MvelExpressionEngine();

    private ExpressionEvaluator() {}

    /** Replace the expression engine at runtime. Thread-safe. */
    public static void setEngine(ExpressionEngine newEngine) {
        engine = newEngine;
    }

    /** Get the current engine name (for diagnostics). */
    public static String engineName() {
        return engine.name();
    }

    /** Evaluate a boolean expression against a Map context. */
    public static boolean evaluateMap(Map<String, Object> row, String expression) {
        return engine.evaluate(row, expression);
    }
}
