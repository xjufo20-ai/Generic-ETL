package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

import java.util.LinkedHashMap;
import java.util.Map;

public class TypeCastStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "typeCast"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.TypeCastDef c = (TransformDef.TypeCastDef) def;
        Map<String, String> map = new LinkedHashMap<>();
        for (var m : c.getMappings()) map.put(m.getField(), m.getToType());
        return "      - setHeader:\n" +
               "          name: casts\n" +
               "          constant: '" + YamlUtils.toJson(map) + "'\n" +
               "      - bean:\n" +
               "          ref: typeCaster\n";
    }
}
