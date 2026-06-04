package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TransformDef.ProjectDef.class, name = "project"),
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

    @Data @EqualsAndHashCode(callSuper = false)
    public static class ProjectDef extends TransformDef {
        private List<MappingDef> mappings;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class FilterDef extends TransformDef {
        private String expression;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class RenameDef extends TransformDef {
        private List<MappingDef> mappings;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class TypeCastDef extends TransformDef {
        private List<TypeCastMapping> mappings;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class AggregateDef extends TransformDef {
        private List<String> groupBy;
        private List<Aggregation> aggregations;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class JoinDef extends TransformDef {
        private String query;
        private String leftKey;
        private String rightKey;
    }
    @Data @EqualsAndHashCode(callSuper = false)
    public static class SplitDef extends TransformDef {
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
