package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

import java.util.LinkedHashMap;
import java.util.Map;

public class RenameStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "rename"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.RenameDef r = (TransformDef.RenameDef) def;
        Map<String, String> map = new LinkedHashMap<>();
        for (var m : r.getMappings()) map.put(m.getFrom(), m.getTo());
        return "      - setHeader:\n" +
               "          name: mappings\n" +
               "          constant: '" + YamlUtils.toJson(map) + "'\n" +
               "      - bean:\n" +
               "          ref: projectTransformer\n";
    }
}
