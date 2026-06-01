package com.generic.etl.common.model;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class PipelineConfig {
    private Pipeline pipeline;
    private DataSourceConfig datasource;
    private SchemaConfig inputSchema;
    private List<TransformDef> transforms;
    private WatermarkConfig watermark;
    private ParallelConfig parallel;
    private PersistConfig output;

    @Data
    public static class Pipeline {
        private String name;
        private String version;
        private String cron;
        private List<String> dependsOn;
    }

    /** Validate the configuration and return a list of issues (empty = valid). */
    public List<String> validate() {
        List<String> issues = new ArrayList<>();
        if (pipeline == null || pipeline.name == null || pipeline.name.isBlank())
            issues.add("pipeline.name is required");
        if (datasource == null)
            issues.add("datasource is required");
        if (inputSchema == null || inputSchema.getFields() == null || inputSchema.getFields().isEmpty())
            issues.add("inputSchema.fields is required");

        // Validate cursor column exists in input schema
        if (datasource instanceof DataSourceConfig.JdbcDataSource jdbc && jdbc.getCursor() != null) {
            Set<String> fieldNames = inputSchema.getFields().stream().map(SchemaConfig.FieldDef::getName).collect(Collectors.toSet());
            if (!fieldNames.contains(jdbc.getCursor().getColumn()))
                issues.add("cursor column '" + jdbc.getCursor().getColumn() + "' not found in inputSchema");
        }

        // Validate transform field references
        if (transforms != null) {
            Set<String> schemaFields = inputSchema != null && inputSchema.getFields() != null
                ? inputSchema.getFields().stream().map(SchemaConfig.FieldDef::getName).collect(Collectors.toSet())
                : Set.of();

            for (TransformDef t : transforms) {
                if (t instanceof TransformDef.RenameDef r && r.getMappings() != null) {
                    for (TransformDef.MappingDef m : r.getMappings()) {
                        if (!schemaFields.contains(m.getFrom()))
                            issues.add("rename: field '" + m.getFrom() + "' not in inputSchema");
                    }
                }
                if (t instanceof TransformDef.TypeCastDef c && c.getMappings() != null) {
                    for (TransformDef.TypeCastMapping m : c.getMappings()) {
                        if (!schemaFields.contains(m.getField()))
                            issues.add("typeCast: field '" + m.getField() + "' not in inputSchema");
                    }
                }
                if (t instanceof TransformDef.AggregateDef a) {
                    if (a.getGroupBy() != null) {
                        for (String gb : a.getGroupBy()) {
                            if (!schemaFields.contains(gb))
                                issues.add("aggregate groupBy: field '" + gb + "' not in inputSchema");
                        }
                    }
                    if (a.getAggregations() != null) {
                        for (TransformDef.Aggregation agg : a.getAggregations()) {
                            if (!schemaFields.contains(agg.getField()))
                                issues.add("aggregate: field '" + agg.getField() + "' not in inputSchema");
                        }
                    }
                }
            }
        }

        return issues;
    }
}
