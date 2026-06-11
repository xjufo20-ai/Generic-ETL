package com.generic.etl.core.compile;

import com.generic.etl.common.model.*;

import java.nio.file.Path;
import java.util.*;

/**
 * Compiles a PipelineConfig into Camel YAML DSL.
 *
 * Design: uses {@link TransformStepCompiler} strategy for each transform type,
 * so adding a new transform only requires a new compiler implementation.
 */
public final class JsonToYamlCompiler {

    private static final Map<String, TransformStepCompiler> TRANSFORM_COMPILERS = new LinkedHashMap<>();

    static {
        List<TransformStepCompiler> compilers = List.of(
                new FilterStepCompiler(),
                new ProjectStepCompiler(),
                new RenameStepCompiler(),
                new TypeCastStepCompiler(),
                new AggregateStepCompiler(),
                new JoinStepCompiler(),
                new SplitStepCompiler()
        );
        for (var c : compilers) TRANSFORM_COMPILERS.put(c.type(), c);
    }

    private JsonToYamlCompiler() {}

    // ── Public API ────────────────────────────────────────────────────

    public static String compile(PipelineConfig config) {
        String name = config.getPipeline().getName();
        StringBuilder yaml = new StringBuilder();

        yaml.append("- route:\n");
        yaml.append("    id: ").append(YamlUtils.quoteYaml(name)).append("\n");
        yaml.append("    from:\n");
        yaml.append(buildSource(config));

        if (config.getDatasource() instanceof DataSourceConfig.CsvDataSource) {
            yaml.append("      - unmarshal:\n");
            yaml.append("          csv:\n");
            yaml.append("            use-maps: true\n");
        } else if (config.getDatasource() instanceof DataSourceConfig.JdbcDataSource j) {
            String q = j.getQuery();
            WatermarkConfig wm = config.getWatermark();
            if (wm != null && wm.getInitial() != null) {
                String clause = q.toUpperCase().contains("WHERE") ? " AND " : " WHERE ";
                q += clause + wm.getColumn() + " >= '" + wm.getInitial() + "'";
            }
            yaml.append("      - setBody:\n");
            yaml.append("          constant: \"").append(YamlUtils.escapeYamlDoubleQuote(q)).append("\"\n");
            yaml.append("      - to:\n");
            yaml.append("          uri: \"sql:?dataSource=#dataSource&outputType=SelectList\"\n");
            // Normalize sql: producer output to List<Map> regardless of Camel version behavior
            yaml.append("      - bean:\n");
            yaml.append("          ref: sqlListToMapList\n");
        }

        // Compile transforms via strategy (with instanceof fallback)
        if (config.getTransforms() != null) {
            for (TransformDef t : config.getTransforms()) {
                TransformStepCompiler compiler = resolveCompiler(t);
                if (compiler != null) {
                    yaml.append(compiler.compile(t));
                }
            }
        }

        yaml.append("      - setProperty:\n");
        yaml.append("          name: pipelineName\n");
        yaml.append("          constant: ").append(YamlUtils.quoteYaml(name)).append("\n");

        yaml.append("      - multicast:\n");
        yaml.append("          steps:\n");
        yaml.append("            - to: bean:loadRouter\n");

        buildOutput(config, yaml);
        return yaml.toString();
    }

    // ── Compiler resolution (type first, instanceof fallback) ─────────

    private static TransformStepCompiler resolveCompiler(TransformDef t) {
        String type = t.getType();
        if (type != null && !type.isBlank()) {
            return TRANSFORM_COMPILERS.get(type);
        }
        if (t instanceof TransformDef.FilterDef) return TRANSFORM_COMPILERS.get("filter");
        if (t instanceof TransformDef.ProjectDef) return TRANSFORM_COMPILERS.get("project");
        if (t instanceof TransformDef.RenameDef) return TRANSFORM_COMPILERS.get("rename");
        if (t instanceof TransformDef.TypeCastDef) return TRANSFORM_COMPILERS.get("typeCast");
        if (t instanceof TransformDef.AggregateDef) return TRANSFORM_COMPILERS.get("aggregate");
        if (t instanceof TransformDef.JoinDef) return TRANSFORM_COMPILERS.get("join");
        if (t instanceof TransformDef.SplitDef) return TRANSFORM_COMPILERS.get("split");
        return null;
    }

    // ── Source ────────────────────────────────────────────────────────

    private static String buildSource(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();
        if (ds instanceof DataSourceConfig.CsvDataSource csv) {
            Path p = Path.of(csv.getFilePath());
            String dir = p.getParent() != null ? p.getParent().toString() : ".";
            String fileName = p.getFileName().toString();
            return "      uri: \"file:" + dir
                    + "?noop=true&idempotent=true"
                    + "&fileName=" + fileName
                    + "&initialDelay=1000"
                    + "&delay=5000\"\n"
                    + "      steps:\n";
        } else if (ds instanceof DataSourceConfig.JdbcDataSource jdbc) {
            return "      uri: \"timer:" + config.getPipeline().getName()
                    + "?period=60000&delay=1000\"\n" +
                   "      steps:\n";
        } else if (ds instanceof DataSourceConfig.KafkaDataSource kafka) {
            var kc = kafka.getConnection();
            return "      uri: \"kafka:" + kc.getTopic()
                    + "?brokers=" + kc.getBootstrapServers()
                    + "&groupId=" + kc.getGroupId() + "\"\n" +
                   "      steps:\n";
        } else if (ds instanceof DataSourceConfig.SftpDataSource sftp) {
            var sc = sftp.getConnection();
            return "      uri: \"sftp:" + sc.getHost() + ":" + sc.getPort()
                    + sc.getDirectory() + "?username=" + sc.getUsername()
                    + "&password=" + sc.getPassword()
                    + "&fileName=" + sftp.getFileName() + "\"\n" +
                   "      steps:\n";
        }
        return "      uri: \"direct:void\"\n      steps:\n";
    }

    // ── Output ────────────────────────────────────────────────────────

    private static void buildOutput(PipelineConfig config, StringBuilder yaml) {
        if (config.getOutput() == null || config.getOutput().getStorage() == null) return;
        var st = config.getOutput().getStorage();

        if ("csv".equalsIgnoreCase(st.getType())) {
            yaml.append("      - marshal:\n");
            yaml.append("          csv:\n");
            yaml.append("            header: ").append(buildCsvHeader(config)).append("\n");
            yaml.append("      - to:\n");
            yaml.append("          uri: \"file:data?fileName=")
             .append(YamlUtils.escapeYamlDoubleQuote(st.getTable())).append("\"\n");
        } else if ("postgresql".equalsIgnoreCase(st.getType()) || "mysql".equalsIgnoreCase(st.getType())) {
            yaml.append("      - bean:\n");
            yaml.append("          ref: rowConverter\n");
            yaml.append("      - setHeader:\n");
            yaml.append("          name: persistTable\n");
            yaml.append("          constant: ").append(YamlUtils.quoteYaml(st.getTable())).append("\n");
            if (st.getPrimaryKeys() != null && !st.getPrimaryKeys().isEmpty()) {
                yaml.append("      - setHeader:\n");
                yaml.append("          name: persistPrimaryKeys\n");
                yaml.append("          constant: ").append(YamlUtils.quoteYaml(String.join(",", st.getPrimaryKeys()))).append("\n");
            }
            yaml.append("      - bean:\n");
            yaml.append("          ref: persistHandler\n");
        }
    }

    static String buildCsvHeader(PipelineConfig config) {
        if (config.getOutputSchema() != null && config.getOutputSchema().getFields() != null) {
            return String.join(",",
                    config.getOutputSchema().getFields().stream()
                            .map(SchemaConfig.FieldDef::getName).toList());
        }
        List<String> headers = deriveHeaders(config);
        return headers.isEmpty() ? "" : String.join(",", headers);
    }

    private static List<String> deriveHeaders(PipelineConfig config) {
        Set<String> fields = new LinkedHashSet<>();
        if (config.getInputSchema() != null && config.getInputSchema().getFields() != null) {
            for (var f : config.getInputSchema().getFields()) fields.add(f.getName());
        }
        if (config.getTransforms() != null) {
            for (TransformDef t : config.getTransforms()) {
                if (t instanceof TransformDef.ProjectDef p && p.getMappings() != null) {
                    fields.clear();
                    for (var m : p.getMappings()) fields.add(m.getTo());
                } else if (t instanceof TransformDef.RenameDef r && r.getMappings() != null) {
                    for (var m : r.getMappings()) {
                        fields.remove(m.getFrom());
                        fields.add(m.getTo());
                    }
                } else if (t instanceof TransformDef.AggregateDef a) {
                    fields.clear();
                    if (a.getGroupBy() != null) fields.addAll(a.getGroupBy());
                    if (a.getAggregations() != null) {
                        for (var ag : a.getAggregations()) {
                            fields.add(ag.getAlias() != null ? ag.getAlias() : ag.getField());
                        }
                    }
                }
            }
        }
        return new ArrayList<>(fields);
    }

    public static String toCamelSimple(String expr) {
        if (expr == null || expr.isBlank()) return "true";
        return expr.replaceAll(
            "\\b([a-zA-Z_]\\w*)\\b(?=\\s*(>=|<=|!=|==|>|<|&&|\\|\\|))",
            "\\${body[$1]}"
        );
    }
}
