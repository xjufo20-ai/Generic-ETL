package com.generic.etl.core.compile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.common.model.*;

import java.nio.file.Path;
import java.util.*;

public final class JsonToYamlCompiler {

    private static final ObjectMapper mapper = new ObjectMapper();

    private JsonToYamlCompiler() {}

    public static String compile(PipelineConfig config) {
        String name = config.getPipeline().getName();
        StringBuilder yaml = new StringBuilder();

        yaml.append("- route:\n");
        yaml.append("    id: ").append(name).append("\n");
        yaml.append("    from:\n");
        yaml.append(buildSource(config));

        if (config.getDatasource() instanceof DataSourceConfig.CsvDataSource) {
            yaml.append("      - unmarshal:\n");
            yaml.append("          csv:\n");
            yaml.append("            use-maps: true\n");
        }

        if (config.getTransforms() != null) {
            for (TransformDef t : config.getTransforms()) {
                yaml.append(buildStep(t));
            }
        }

        yaml.append("      - setProperty:\n");
        yaml.append("          name: pipelineName\n");
        yaml.append("          constant: ").append(name).append("\n");

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
            // Need to marshal List<Map> back to CSV before file write
            yaml.append("            - marshal:\n");
            yaml.append("                csv:\n");
            yaml.append("                  header: ").append(buildCsvHeader(config)).append("\n");
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

    private static String buildCsvHeader(PipelineConfig config) {
        // Derive header from outputSchema or inputSchema field names
        if (config.getOutputSchema() != null && config.getOutputSchema().getFields() != null) {
            return String.join(",", config.getOutputSchema().getFields().stream().map(f -> f.getName()).toList());
        }
        if (config.getInputSchema() != null && config.getInputSchema().getFields() != null) {
            return String.join(",", config.getInputSchema().getFields().stream().map(f -> f.getName()).toList());
        }
        return "";
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
            Path p = Path.of(c.getFilePath());
            String dir = p.getParent() != null ? p.getParent().toString() : ".";
            String fn = p.getFileName().toString();
            s.append("      uri: file:").append(dir).append("\n");
            s.append("      parameters:\n");
            s.append("        fileName: ").append(fn).append("\n");
            s.append("        noop: true\n");
            s.append("        idempotent: false\n");
        } else if (ds instanceof DataSourceConfig.SftpDataSource sf) {
            s.append("      uri: sftp://").append(sf.getConnection().getUsername())
             .append("@").append(sf.getConnection().getHost()).append(":")
             .append(sf.getConnection().getPort()).append(sf.getConnection().getDirectory())
             .append("?password=").append(sf.getConnection().getPassword())
             .append("&fileName=").append(sf.getFileName() != null ? sf.getFileName() : "*.*").append("\n");
        }
        s.append("      steps:\n");
        return s.toString();
    }

    private static String buildStep(TransformDef t) {
        StringBuilder s = new StringBuilder();
        if (t instanceof TransformDef.ProjectDef p) {
            s.append(setHeaderJson("mappings", buildMappingsJson(p.getMappings())));
            s.append("      - bean:\n");
            s.append("          ref: projectTransformer\n");
        } else if (t instanceof TransformDef.FilterDef f) {
            s.append("      - setHeader:\n");
            s.append("          name: filterExpr\n");
            s.append("          constant: '").append(escapeJson(f.getExpression())).append("'\n");
            s.append("      - bean:\n");
            s.append("          ref: rowFilter\n");
        } else if (t instanceof TransformDef.RenameDef r) {
            s.append(setHeaderJson("mappings", buildMappingsJson(r.getMappings())));
            s.append("      - bean:\n");
            s.append("          ref: projectTransformer\n");
        } else if (t instanceof TransformDef.AggregateDef a) {
            s.append(setHeaderJson("etlAgg", buildAggJson(a)));
            s.append("      - bean:\n");
            s.append("          ref: etlAggregator\n");
        } else if (t instanceof TransformDef.TypeCastDef c) {
            s.append(setHeaderJson("casts", buildCastsJson(c.getMappings())));
            s.append("      - bean:\n");
            s.append("          ref: typeCaster\n");
        } else if (t instanceof TransformDef.JoinDef j) {
            s.append("      - enrich:\n");
            s.append("          uri: jdbc:etlDataSource?query=").append(escapeJson(j.getQuery())).append("&outputType=SelectOne\n");
        } else if (t instanceof TransformDef.SplitDef) {
            s.append("      - split:\n");
            s.append("          expression: ${body}\n");
        }
        return s.toString();
    }

    private static String setHeaderJson(String name, String json) {
        return "      - setHeader:\n" +
               "          name: " + name + "\n" +
               "          constant: '" + json + "'\n";
    }

    private static String buildMappingsJson(List<TransformDef.MappingDef> mappings) {
        Map<String, String> map = new LinkedHashMap<>();
        for (var m : mappings) map.put(m.getFrom(), m.getTo());
        return toJson(map);
    }

    private static String buildCastsJson(List<TransformDef.TypeCastMapping> mappings) {
        Map<String, String> map = new LinkedHashMap<>();
        for (var m : mappings) map.put(m.getField(), m.getToType());
        return toJson(map);
    }

    private static String buildAggJson(TransformDef.AggregateDef a) {
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
        return toJson(config);
    }

    private static String toJson(Object obj) {
        try { return mapper.writeValueAsString(obj); } catch (JsonProcessingException e) { return "{}"; }
    }

    static String toCamelSimple(String expr) {
        if (expr == null || expr.isBlank()) return "true";
        return expr.replaceAll(
            "\\b([a-zA-Z_]\\w*)\\b(?=\\s*(>=|<=|!=|==|>|<|&&|\\|\\|))",
            "\\${body[$1]}"
        );
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'");
    }
}
