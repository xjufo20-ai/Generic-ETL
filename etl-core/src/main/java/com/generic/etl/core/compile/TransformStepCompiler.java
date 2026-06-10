package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

/**
 * Strategy pattern: each transform type knows how to compile itself into Camel YAML DSL.
 */
public interface TransformStepCompiler {
    /** The transform type identifier this compiler handles (e.g. "filter", "project"). */
    String type();

    /** Compile a single transform step into YAML DSL lines. */
    String compile(TransformDef def);
}
