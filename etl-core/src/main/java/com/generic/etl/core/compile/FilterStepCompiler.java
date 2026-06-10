package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

public class FilterStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "filter"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.FilterDef f = (TransformDef.FilterDef) def;
        return "      - setHeader:\n" +
               "          name: filterExpr\n" +
               "          constant: \"" + YamlUtils.escapeYamlDoubleQuote(f.getExpression()) + "\"\n" +
               "      - bean:\n" +
               "          ref: rowFilter\n";
    }
}
