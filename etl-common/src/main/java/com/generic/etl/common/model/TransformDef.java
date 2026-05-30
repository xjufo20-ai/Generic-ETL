package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
public class TransformDef {
    private String type; // filter, rename, typeCast, aggregate, join
    private String expression;

    @JsonProperty("mappings")
    private List<MappingDef> mappings;

    @JsonProperty("groupBy")
    private List<String> groupBy;

    @JsonProperty("aggregations")
    private List<AggregationDef> aggregations;

    private String query;
    private String on;
    private String joinType; // INNER, LEFT

    @Data
    public static class MappingDef {
        private String from;
        private String to;
        private String field;
        private String toType;
    }

    @Data
    public static class AggregationDef {
        private String field;
        private String function; // SUM, AVG, COUNT, MIN, MAX
        private String alias;
    }
}
