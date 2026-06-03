package com.generic.etl.common.model;

import lombok.Data;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class PipelineConfig {
    private Pipeline pipeline;
    private DataSourceConfig datasource;
    private SchemaConfig inputSchema;
    private List<TransformDef> transforms;
    private SchemaConfig outputSchema;
    private WatermarkConfig watermark;
    private PersistConfig output;

    @Data public static class Pipeline { private String name; private String version; private String cron; }

    public List<String> validate() {
        List<String> issues = new ArrayList<>();
        if (pipeline == null || pipeline.name == null || pipeline.name.isBlank())
            issues.add("pipeline.name is required");
        if (datasource == null) issues.add("datasource is required");
        if (inputSchema == null || inputSchema.getFields() == null || inputSchema.getFields().isEmpty())
            issues.add("inputSchema.fields is required");

        Set<String> inputFields = inputSchema.getFields().stream()
                .map(SchemaConfig.FieldDef::getName).collect(Collectors.toSet());
        Set<String> canonicalFields = inputFields;
        if (transforms != null && !transforms.isEmpty() && transforms.get(0) instanceof TransformDef.ProjectDef p) {
            canonicalFields = p.getMappings().stream()
                    .map(TransformDef.MappingDef::getTo).collect(Collectors.toSet());
        }

        // Validate cursor
        if (datasource instanceof DataSourceConfig.JdbcDataSource j && j.getCursor() != null) {
            if (!inputFields.contains(j.getCursor().getColumn()))
                issues.add("cursor column '" + j.getCursor().getColumn() + "' not in inputSchema");
        }

        // Validate transforms against canonical fields
        if (transforms != null) {
            for (TransformDef t : transforms) {
                if (t instanceof TransformDef.RenameDef r && r.getMappings() != null)
                    for (var m : r.getMappings())
                        if (!canonicalFields.contains(m.getFrom()))
                            issues.add("rename: '" + m.getFrom() + "' not in schema");
                if (t instanceof TransformDef.TypeCastDef c && c.getMappings() != null)
                    for (var m : c.getMappings())
                        if (!canonicalFields.contains(m.getField()))
                            issues.add("typeCast: '" + m.getField() + "' not in schema");
                if (t instanceof TransformDef.AggregateDef a) {
                    if (a.getGroupBy() != null)
                        for (String gb : a.getGroupBy())
                            if (!canonicalFields.contains(gb))
                                issues.add("aggregate groupBy: '" + gb + "' not in schema");
                    if (a.getAggregations() != null)
                        for (var agg : a.getAggregations())
                            if (!canonicalFields.contains(agg.getField()))
                                issues.add("aggregate: '" + agg.getField() + "' not in schema");
                }
            }
        }
        if (outputSchema != null && outputSchema.getFields() != null)
            for (var f : outputSchema.getFields())
                if (!canonicalFields.contains(f.getName()))
                    issues.add("outputSchema: '" + f.getName() + "' not produced by transforms");

        return issues;
    }
}
