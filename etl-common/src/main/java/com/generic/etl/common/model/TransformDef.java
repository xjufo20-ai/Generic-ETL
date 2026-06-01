package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TransformDef.FilterDef.class, name = "filter"),
    @JsonSubTypes.Type(value = TransformDef.RenameDef.class, name = "rename"),
    @JsonSubTypes.Type(value = TransformDef.TypeCastDef.class, name = "typeCast"),
    @JsonSubTypes.Type(value = TransformDef.AggregateDef.class, name = "aggregate"),
    @JsonSubTypes.Type(value = TransformDef.JoinDef.class, name = "join"),
    @JsonSubTypes.Type(value = TransformDef.SplitDef.class, name = "split")
})
@Data
public abstract class TransformDef {
    protected String type;

    @Data
    public static class FilterDef extends TransformDef {
        public FilterDef() { this.type = "filter"; }
        private String expression;
    }

    @Data
    public static class RenameDef extends TransformDef {
        public RenameDef() { this.type = "rename"; }
        @JsonProperty("mappings")
        private List<MappingDef> mappings;
    }

    @Data
    public static class TypeCastDef extends TransformDef {
        public TypeCastDef() { this.type = "typeCast"; }
        @JsonProperty("mappings")
        private List<TypeCastMapping> mappings;
    }

    @Data
    public static class AggregateDef extends TransformDef {
        public AggregateDef() { this.type = "aggregate"; }
        @JsonProperty("groupBy")
        private List<String> groupBy;
        @JsonProperty("aggregations")
        private List<Aggregation> aggregations;
    }

    @Data
    public static class JoinDef extends TransformDef {
        public JoinDef() { this.type = "join"; }
        private String query;
        private String leftKey;
        private String rightKey;
        private String joinType;
    }

    @Data
    public static class SplitDef extends TransformDef {
        public SplitDef() { this.type = "split"; }
        private String field;
        private String delimiter;
    }

    @Data
    public static class MappingDef {
        private String from;
        private String to;
    }

    @Data
    public static class TypeCastMapping {
        private String field;
        private String toType;
    }

    @Data
    public static class Aggregation {
        private String field;
        private String function;
        private String alias;
    }
}
