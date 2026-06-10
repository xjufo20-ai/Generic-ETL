package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

import java.util.LinkedHashMap;
import java.util.Map;

public class ProjectStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "project"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.ProjectDef p = (TransformDef.ProjectDef) def;
        Map<String, String> map = new LinkedHashMap<>();
        for (var m : p.getMappings()) map.put(m.getFrom(), m.getTo());
        return "      - setHeader:\n" +
               "          name: mappings\n" +
               "          constant: '" + YamlUtils.toJson(map) + "'\n" +
               "      - bean:\n" +
               "          ref: projectTransformer\n";
    }
}
