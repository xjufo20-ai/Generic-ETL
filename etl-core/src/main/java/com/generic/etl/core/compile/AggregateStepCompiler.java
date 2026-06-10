package com.generic.etl.core.compile;

import com.generic.etl.common.model.TransformDef;

import java.util.*;

public class AggregateStepCompiler implements TransformStepCompiler {
    @Override
    public String type() { return "aggregate"; }

    @Override
    public String compile(TransformDef def) {
        TransformDef.AggregateDef a = (TransformDef.AggregateDef) def;
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("groupBy", a.getGroupBy() != null ? a.getGroupBy() : List.of());
        if (a.getAggregations() != null) {
            List<Map<String, String>> aggs = new ArrayList<>();
            for (var ag : a.getAggregations()) {
                Map<String, String> m = new LinkedHashMap<>();
                m.put("field", ag.getField());
                m.put("function", ag.getFunction());
                m.put("alias", ag.getAlias() != null ? ag.getAlias() : ag.getField());
                aggs.add(m);
            }
            config.put("aggregations", aggs);
        }
        return "      - setHeader:\n" +
               "          name: etlAgg\n" +
               "          constant: '" + YamlUtils.toJson(config) + "'\n" +
               "      - bean:\n" +
               "          ref: etlAggregator\n";
    }
}
