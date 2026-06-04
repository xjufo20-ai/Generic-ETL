package com.generic.etl.api.config;

import com.generic.etl.common.model.*;

/**
 * Compiles PipelineConfig JSON → Camel YAML DSL string.
 * Thin translator — no RouteBuilder. Camel reads the YAML natively.
 *
 * Each generated route includes an onException block that delegates to
 * camelDeadLetterHandler for error collection and logging.
 */
public final class JsonToYamlCompiler {

    private JsonToYamlCompiler() {}

    public static String compile(PipelineConfig config) {
        String name = config.getPipeline().getName();
        StringBuilder yaml = new StringBuilder();

        yaml.append("- route:\n");
        yaml.append("    id: ").append(name).append("\n");

        // ── Error handling: delegate to CamelDeadLetterHandler ──
        yaml.append("    onException:\n");
        yaml.append("      - exception: java.lang.Exception\n");
        yaml.append("        handled: true\n");
        yaml.append("        steps:\n");
        yaml.append("          - setProperty:\n");
        yaml.append("              name: pipelineName\n");
        yaml.append("              constant: ").append(name).append("\n");
        yaml.append("          - bean:\n");
        yaml.append("              ref: camelDeadLetterHandler\n");

        yaml.append("    from:\n");
        yaml.append(buildSource(config));
        yaml.append("    steps:\n");

        if (config.getTransforms() != null) {
            for (TransformDef t : config.getTransforms()) {
                yaml.append(buildStep(t));
            }
        }

        // pipelineName for LoadRouter
        yaml.append("      - setProperty:\n");
        yaml.append("          name: pipelineName\n");
        yaml.append("          constant: ").append(name).append("\n");

        // Output: multicast
        yaml.append("      - multicast:\n");
        yaml.append("          steps:\n");
        yaml.append("            - to: bean:loadRouter\n");
        buildOutput(config, yaml);

        return yaml.toString();
    }

    private static void buildOutput(PipelineConfig config, StringBuilder yaml) {
        if (config.getOutput() == null || config.getOutput().getStorage() == null) return;
        var st = config.getOutput().getStorage();
        if ("csv".equalsIgnoreCase(st.getType())) {
            yaml.append("            - to:\n");
            yaml.append("                uri: file:data\n");
            yaml.append("                parameters:\n");
            yaml.append("                  fileName: ").append(st.getTable()).append("\n");
        } else if ("postgresql".equalsIgnoreCase(st.getType()) || "mysql".equalsIgnoreCase(st.getType())) {
            yaml.append("            - bean:\n");
            yaml.append("                ref: persistHandler\n");
            yaml.append("                parameters:\n");
            yaml.append("                  table: ").append(st.getTable()).append("\n");
            if (st.getPrimaryKeys() != null && !st.getPrimaryKeys().isEmpty()) {
                yaml.append("                  primaryKeys: ").append(String.join(",", st.getPrimaryKeys())).append("\n");
            }
        }
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
            s.append("        query: >\n          ").append(q).append("\n");
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
            s.append("      - bean:\n          ref: projectTransformer\n          parameters:\n            mappings:\n");
            for (var m : p.getMappings())
                s.append("              ").append(m.getFrom()).append(": ").append(m.getTo()).append("\n");
        } else if (t instanceof TransformDef.FilterDef f) {
            String simple = toCamelSimple(f.getExpression());
            s.append("      - filter:\n          simple: \"").append(simple).append("\"\n");
        } else if (t instanceof TransformDef.RenameDef r) {
            s.append("      - bean:\n          ref: projectTransformer\n          parameters:\n            mappings:\n");
            for (var m : r.getMappings()) s.append("              ").append(m.getFrom()).append(": ").append(m.getTo()).append("\n");
        } else if (t instanceof TransformDef.AggregateDef a) {
            s.append("      - bean:\n          ref: etlAggregator\n          parameters:\n");
            if (a.getGroupBy() != null && !a.getGroupBy().isEmpty())
                s.append("            groupBy: ").append(String.join(",", a.getGroupBy())).append("\n");
            s.append("            aggregations:\n");
            for (var agg : a.getAggregations())
                s.append("              - {field: ").append(agg.getField())
                 .append(", function: ").append(agg.getFunction())
                 .append(", alias: ").append(agg.getAlias() != null ? agg.getAlias() : agg.getField()).append("}\n");
        } else if (t instanceof TransformDef.TypeCastDef c) {
            s.append("      - bean:\n          ref: typeCaster\n          parameters:\n            casts:\n");
            for (var m : c.getMappings()) s.append("              ").append(m.getField()).append(": ").append(m.getToType()).append("\n");
        } else if (t instanceof TransformDef.JoinDef j) {
            s.append("      - enrich:\n          uri: jdbc:etlDataSource?query=").append(j.getQuery()).append("&outputType=SelectOne\n");
        } else if (t instanceof TransformDef.SplitDef) {
            s.append("      - split:\n          expression: ${body}\n");
        }
        return s.toString();
    }

    /**
     * Convert MVEL expression to Camel Simple:
     *   "salary > 5000"              → "${body[salary]} > 5000"
     *   "a > 5 && b < 10"           → "${body[a]} > 5 && ${body[b]} < 10"
     *   "name == 'John'"            → "${body[name]} == 'John'"
     *   "dept != 'Sales'"           → "${body[dept]} != 'Sales'"
     */
    static String toCamelSimple(String expr) {
        if (expr == null || expr.isBlank()) return "true";
        // Wrap bare identifiers that precede comparison/logical operators
        // Covers: >, <, >=, <=, ==, !=, &&, ||
        return expr.replaceAll(
            "\\b([a-zA-Z_]\\w*)\\b(?=\\s*(>=|<=|!=|==|>|<|&&|\\|\\|))",
            "\\${body[$1]}"
        );
    }
}
