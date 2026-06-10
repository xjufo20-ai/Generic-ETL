package com.generic.etl.core.expression;

import java.util.Map;

/**
 * Pluggable expression evaluation engine.
 * Default implementation uses MVEL; alternative engines (SpEL, Groovy, JS)
 * can be provided by implementing this interface.
 */
public interface ExpressionEngine {
    /** Human-readable engine name for diagnostics. */
    String name();

    /** Compile and evaluate a boolean expression against a context map. */
    boolean evaluate(Map<String, Object> context, String expression);
}
