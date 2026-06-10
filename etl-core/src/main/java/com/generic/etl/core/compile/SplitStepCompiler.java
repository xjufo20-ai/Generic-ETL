package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

public class SplitStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "split"; }

    @Override
    public String compile(TransformDef def) {
        return "      - split:\n" +
               "          expression: ${body}\n";
    }
}
