package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

public class JoinStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "join"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.JoinDef j = (TransformDef.JoinDef) def;
        return "      - setBody:\n" +
               "          constant: \"" + YamlUtils.escapeYamlDoubleQuote(j.getQuery()) + "\"\n" +
               "      - enrich:\n" +
               "          expression:\n" +
               "            constant: \"sql:?dataSource=#dataSource&outputType=SelectOne\"\n" +
               // Normalize enrich result (may be String, Map, or List<Map>)
               "      - bean:\n" +
               "          ref: sqlListToMapList\n";
    }
}
