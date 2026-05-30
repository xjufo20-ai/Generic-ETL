package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.expression.ExpressionEvaluator;
import com.generic.etl.core.transform.TransformProcessor;

public class FilterProcessor implements TransformProcessor {
    @Override
    public Row process(Row row, TransformDef def) {
        if (def instanceof TransformDef.FilterDef f) {
            if (ExpressionEvaluator.evaluate(row, f.getExpression())) return row;
        }
        return null;
    }
}
