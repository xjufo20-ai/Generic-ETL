package com.generic.etl.core.expression;

import com.generic.etl.common.model.Row;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;

public class ExpressionEvaluator {

    public static boolean evaluate(Row row, String expression) {
        if (expression == null || expression.isBlank()) {
            return true;
        }
        try {
            Serializable compiled = MVEL.compileExpression(expression);
            Object result = MVEL.executeExpression(compiled, row.getValues());
            if (result instanceof Boolean b) {
                return b;
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate expression: " + expression, e);
        }
    }

    public static Object evaluateValue(Row row, String expression) {
        if (expression == null || expression.isBlank()) {
            return null;
        }
        try {
            Serializable compiled = MVEL.compileExpression(expression);
            return MVEL.executeExpression(compiled, row.getValues());
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate expression: " + expression, e);
        }
    }
}
