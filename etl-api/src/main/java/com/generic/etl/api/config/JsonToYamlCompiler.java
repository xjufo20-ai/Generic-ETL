package com.generic.etl.api.config;

import com.generic.etl.common.model.*;

/**
 * Compiles PipelineConfig JSON → Camel YAML DSL string.
 * Thin ~100-line translator — no RouteBuilder, no EIP mapper.
 * Camel reads the resulting YAML natively.
 */
public class JsonToYamlCompiler {

    public static String compile(PipelineConfig config) {
        String name = config.getPipeline().getName();
        StringBuilder yaml = new StringBuilder();

        yaml.append("- route:\n");
        yaml.append("    id: ").append(name).append("\n");

        // Source
        yaml.append("    from:\n");
        yaml.append(buildSource(config));

        // Transforms
        yaml.append("    steps:\n");
        if (config.getTransforms() != null) {
            for (TransformDef t : config.getTransforms()) {
                yaml.append(buildStep(t));
            }
        }

        // Set pipelineName for LoadRouter
        yaml.append("      - setProperty:\n");
        yaml.append("          name: pipelineName\n");
        yaml.append("          constant: ").append(name).append("\n");

        // Output: multicast to loadRouter (+ CSV if configured)
        yaml.append("      - multicast:\n");
        yaml.append("          steps:\n");
        yaml.append("            - to: bean:loadRouter\n");
        if (config.getOutput() != null && config.getOutput().getStorage() != null
                && "csv".equalsIgnoreCase(config.getOutput().getStorage().getType())) {
            yaml.append("            - to:\n");
            yaml.append("                uri: file:data\n");
            yaml.append("                parameters:\n");
            yaml.append("                  fileName: ").append(config.getOutput().getStorage().getTable()).append("\n");
        }

        return yaml.toString();
    }

    private static String buildSource(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();
        StringBuilder s = new StringBuilder();

        if (ds instanceof DataSourceConfig.JdbcDataSource j) {
            String q = j.getQuery();
            if (config.getWatermark() != null && config.getWatermark().getInitial() != null) {
                q += " AND " + config.getWatermark().getColumn() + " >= '" + config.getWatermark().getInitial() + "'";
            }
            s.append("      uri: jdbc:etlDataSource\n");
            s.append("      parameters:\n");
            s.append("        query: >\n");
            s.append("          ").append(q).append("\n");
        } else if (ds instanceof DataSourceConfig.KafkaDataSource k) {
            s.append("      uri: kafka:").append(k.getConnection().getTopic())
             .append("?brokers=").append(k.getConnection().getBootstrapServers())
             .append("&groupId=").append(k.getConnection().getGroupId()).append("\n");
        } else if (ds instanceof DataSourceConfig.CsvDataSource c) {
            s.append("      uri: file:").append(c.getFilePath()).append("?noop=true\n");
        } else if (ds instanceof DataSourceConfig.SftpDataSource sf) {
            s.append("      uri: sftp://").append(sf.getConnection().getUsername())
             .append("@").append(sf.getConnection().getHost()).append(":")
             .append(sf.getConnection().getPort()).append(sf.getConnection().getDirectory())
             .append("?password=").append(sf.getConnection().getPassword())
             .append("&fileName=").append(sf.getFileName() != null ? sf.getFileName() : "*.*").append("\n");
        }
        return s.toString();
    }

    private static String buildStep(TransformDef t) {
        StringBuilder s = new StringBuilder();
        if (t instanceof TransformDef.ProjectDef p) {
            s.append("      - bean:\n");
            s.append("          ref: projectTransformer\n");
            s.append("          parameters:\n");
            s.append("            mappings:\n");
            for (var m : p.getMappings()) {
                s.append("              ").append(m.getFrom()).append(": ").append(m.getTo()).append("\n");
            }
        } else if (t instanceof TransformDef.FilterDef f) {
            s.append("      - filter:\n");
            s.append("          simple: \"${body[").append(extractField(f.getExpression())).append("]}")
             .append(" ").append(extractOp(f.getExpression())).append("\"\n");
        } else if (t instanceof TransformDef.RenameDef r) {
            s.append("      - bean:\n");
            s.append("          ref: projectTransformer\n");
            s.append("          parameters:\n");
            s.append("            mappings:\n");
            for (var m : r.getMappings()) s.append("              ").append(m.getFrom()).append(": ").append(m.getTo()).append("\n");
        } else if (t instanceof TransformDef.AggregateDef a) {
            s.append("      - bean:\n");
            s.append("          ref: etlAggregator\n");
            s.append("          parameters:\n");
            if (a.getGroupBy() != null && !a.getGroupBy().isEmpty())
                s.append("            groupBy: ").append(String.join(",", a.getGroupBy())).append("\n");
            s.append("            aggregations:\n");
            for (var agg : a.getAggregations())
                s.append("              - {field: ").append(agg.getField())
                 .append(", function: ").append(agg.getFunction())
                 .append(", alias: ").append(agg.getAlias() != null ? agg.getAlias() : agg.getField()).append("}\n");
        } else if (t instanceof TransformDef.TypeCastDef c) {
            s.append("      - bean:\n");
            s.append("          ref: typeCaster\n");
            s.append("          parameters:\n");
            s.append("            casts:\n");
            for (var m : c.getMappings())
                s.append("              ").append(m.getField()).append(": ").append(m.getToType()).append("\n");
        } else if (t instanceof TransformDef.JoinDef j) {
            s.append("      - enrich:\n");
            s.append("          uri: jdbc:etlDataSource?query=").append(j.getQuery()).append("&outputType=SelectOne\n");
        } else if (t instanceof TransformDef.SplitDef) {
            s.append("      - split:\n");
            s.append("          expression: ${body}\n");
        }
        return s.toString();
    }

    // Simple MVEL expression parsing helpers
    private static String extractField(String expr) {
        String[] parts = expr.split("[><=! ]+");
        return parts.length > 0 ? parts[0].trim() : "value";
    }
    private static String extractOp(String expr) {
        if (expr.contains(">=")) return ">= value";
        if (expr.contains("<=")) return "<= value";
        if (expr.contains("!=")) return "!= value";
        if (expr.contains("==")) return "== value";
        if (expr.contains(">")) return "> value";
        if (expr.contains("<")) return "< value";
        return "!= null";
    }
}
