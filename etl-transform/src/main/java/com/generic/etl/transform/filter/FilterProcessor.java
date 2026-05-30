package com.generic.etl.transform.filter;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.expression.ExpressionEvaluator;
import com.generic.etl.core.transform.TransformProcessor;

public class FilterProcessor implements TransformProcessor {

    @Override
    public Row process(Row row, TransformDef def) {
        if (ExpressionEvaluator.evaluate(row, def.getExpression())) {
            return row;
        }
        return null; // filtered out
    }
}
