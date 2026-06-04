package com.generic.etl.core.config;

import com.generic.etl.common.model.*;
import java.util.*;
import java.util.stream.Collectors;

/** Validates a PipelineConfig before compilation. */
public class PipelineConfigValidator {

    public List<String> validate(PipelineConfig config) {
        List<String> issues = new ArrayList<>();
        if (config.getPipeline() == null || config.getPipeline().getName() == null
                || config.getPipeline().getName().isBlank())
            issues.add("pipeline.name is required");
        if (config.getDatasource() == null) issues.add("datasource is required");
        if (config.getInputSchema() == null || config.getInputSchema().getFields() == null
                || config.getInputSchema().getFields().isEmpty())
            issues.add("inputSchema.fields is required");

        Set<String> inputFields = config.getInputSchema().getFields().stream()
                .map(SchemaConfig.FieldDef::getName).collect(Collectors.toSet());
        Set<String> canonicalFields = inputFields;
        if (config.getTransforms() != null && !config.getTransforms().isEmpty()
                && config.getTransforms().get(0) instanceof TransformDef.ProjectDef p) {
            canonicalFields = p.getMappings().stream()
                    .map(TransformDef.MappingDef::getTo).collect(Collectors.toSet());
        }

        validateCursor(config, inputFields, issues);
        validateTransforms(config, canonicalFields, issues);
        validateOutputSchema(config, canonicalFields, issues);

        return issues;
    }

    private void validateCursor(PipelineConfig config, Set<String> inputFields, List<String> issues) {
        if (config.getDatasource() instanceof DataSourceConfig.JdbcDataSource j && j.getCursor() != null) {
            if (!inputFields.contains(j.getCursor().getColumn()))
                issues.add("cursor column '" + j.getCursor().getColumn() + "' not in inputSchema");
        }
    }

    private void validateTransforms(PipelineConfig config, Set<String> canonicalFields, List<String> issues) {
        if (config.getTransforms() == null) return;
        for (TransformDef t : config.getTransforms()) {
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

    private void validateOutputSchema(PipelineConfig config, Set<String> canonicalFields, List<String> issues) {
        if (config.getOutputSchema() != null && config.getOutputSchema().getFields() != null)
            for (var f : config.getOutputSchema().getFields())
                if (!canonicalFields.contains(f.getName()))
                    issues.add("outputSchema: '" + f.getName() + "' not produced by transforms");
    }
}
