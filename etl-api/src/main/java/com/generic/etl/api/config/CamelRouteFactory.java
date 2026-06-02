package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.common.model.*;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.expression.ExpressionEvaluator;
import com.generic.etl.load.LoadRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;

import java.util.*;

/**
 * Maps PipelineConfig → Camel Route with native EIP patterns.
 *
 * Transform → Camel EIP mapping:
 *   filter    → .filter(predicate)
 *   rename    → .process(map fields)
 *   typeCast  → .process(cast types)
 *   aggregate → .aggregate(GroupedBodyStrategy)
 *   join      → .enrich(jdbc:...)
 *   split     → .split(body())
 *
 * Then: .multicast().to(loadRouter).to(metrics+audit).
 */
@Slf4j
public class CamelRouteFactory {

    private final CamelContext camelContext;
    private final PipelineConfigParser configParser;
    private final EtlMetrics metrics;
    private final AuditLog auditLog;
    private final LineageStore lineageStore;
    private final LoadRouter loadRouter;
    private final Set<String> registeredPipelines = new HashSet<>();

    public CamelRouteFactory(CamelContext camelContext, PipelineConfigParser configParser,
                              EtlMetrics metrics, AuditLog auditLog, LineageStore lineageStore,
                              LoadRouter loadRouter) {
        this.camelContext = camelContext;
        this.configParser = configParser;
        this.metrics = metrics;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
        this.loadRouter = loadRouter;
    }

    // ── Public API ────────────────────────────────────────────

    /** Register a pipeline as a Camel route with native EIP. */
    public synchronized void register(PipelineConfig config) throws Exception {
        String name = config.getPipeline().getName();
        String routeId = routeId(name);
        unregister(name);

        RouteBuilder rb = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Exception.class)
                    .handled(true).maximumRedeliveries(3).redeliveryDelay(5000)
                    .to("bean:camelDeadLetterHandler")
                    .log("FAILED: ${exchangeProperty.pipelineName} — ${exception.message}");

                // ── Source ──
                from(buildSourceUri(config))
                    .routeId(routeId)
                    .setProperty("pipelineName", constant(name))
                    .setProperty("startTime", simple("${date:now}"))
                    .process(e -> {
                        Object b = e.getIn().getBody();
                        if (!(b instanceof List)) e.getIn().setBody(b != null ? List.of(b) : List.of());
                    });

                // ── Transforms: each becomes a native Camel EIP step ──
                if (config.getTransforms() != null) {
                    for (TransformDef def : config.getTransforms()) {
                        buildEip(config, def);
                    }
                }

                // ── Split back to List for loadRouter ──
                .process(e -> {
                    Object b = e.getIn().getBody();
                    if (b instanceof Map) e.getIn().setBody(List.of(b));
                    else if (!(b instanceof List)) e.getIn().setBody(List.of());
                })

                // ── Load: multicast ──
                .multicast().parallelProcessing()
                    .to("bean:loadRouter?method=route")
                .end()

                // ── Metrics + Audit ──
                .process(e -> {
                    String pn = e.getProperty("pipelineName", String.class);
                    int rows = e.getIn().getBody(List.class).size();
                    long dur = System.currentTimeMillis() - e.getProperty("startTime", Long.class);
                    auditLog.recordExecution(pn, "SUCCESS", rows, "system");
                    metrics.recordSuccess(pn, rows, dur);
                    String tbl = config.getOutput() != null && config.getOutput().getStorage() != null
                            ? config.getOutput().getStorage().getTable() : "camel";
                    lineageStore.record(pn, tbl, "default", rows, "SUCCESS");
                })
                .log("OK: ${exchangeProperty.pipelineName} — ${body.size} rows");
            }

            @SuppressWarnings("unchecked")
            private void buildEip(PipelineConfig cfg, TransformDef def) {
                switch (def) {
                    case TransformDef.FilterDef f -> {
                        String expr = f.getExpression();
                        filter(e -> {
                            Map<String, Object> row = e.getIn().getBody(Map.class);
                            return row != null && ExpressionEvaluator.evaluateMap(row, expr);
                        });
                    }
                    case TransformDef.RenameDef r -> {
                        process(e -> {
                            Map<String, Object> row = e.getIn().getBody(Map.class);
                            if (row != null && r.getMappings() != null) {
                                for (var m : r.getMappings()) {
                                    Object v = row.remove(m.getFrom());
                                    row.put(m.getTo(), v);
                                }
                            }
                        });
                    }
                    case TransformDef.TypeCastDef c -> {
                        process(e -> {
                            Map<String, Object> row = e.getIn().getBody(Map.class);
                            if (row != null && c.getMappings() != null) {
                                for (var m : c.getMappings())
                                    row.computeIfPresent(m.getField(), (k, v) -> cast(v, m.getToType()));
                            }
                        });
                    }
                    case TransformDef.AggregateDef a -> {
                        List<String> gb = a.getGroupBy() != null ? a.getGroupBy() : List.of();
                        List<TransformDef.Aggregation> aggs = a.getAggregations() != null ? a.getAggregations() : List.of();
                        aggregate(header("pipelineName"), new GroupedBodyAggregationStrategy())
                            .completionTimeout(5000);
                        process(e -> {
                            List<Map<String, Object>> group = e.getIn().getBody(List.class);
                            e.getIn().setBody(List.of(aggregateRows(group, aggs, gb)));
                        });
                    }
                    case TransformDef.JoinDef j -> {
                        enrich("jdbc:etlDataSource?query=" + j.getQuery() + "&outputType=SelectOne",
                            (oldEx, newEx) -> {
                                Map<String, Object> row = oldEx.getIn().getBody(Map.class);
                                Map<String, Object> enriched = newEx.getIn().getBody(Map.class);
                                if (row != null && enriched != null) row.putAll(enriched);
                                return oldEx;
                            });
                    }
                    case TransformDef.SplitDef s -> split(body());
                    default -> log.warn("Unknown transform type: {}", def.getType());
                }
            }
        };

        camelContext.addRoutes(rb);
        registeredPipelines.add(name);
        log.info("Registered Camel EIP route: {}", routeId);
    }

    public synchronized void unregister(String name) {
        String routeId = routeId(name);
        try {
            if (camelContext.getRoute(routeId) != null) {
                camelContext.getRouteController().stopRoute(routeId);
                camelContext.removeRoute(routeId);
            }
        } catch (Exception e) { log.warn("Unregister {}: {}", routeId, e.getMessage()); }
        registeredPipelines.remove(name);
    }

    public void execute(String name) {
        camelContext.createProducerTemplate().sendBody("direct:trigger-" + name, null);
    }
    public boolean isRegistered(String name) { return registeredPipelines.contains(name); }
    public Set<String> getRegisteredPipelines() { return Collections.unmodifiableSet(registeredPipelines); }

    // ── URI Builder ───────────────────────────────────────────
    String buildSourceUri(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();
        if (ds instanceof DataSourceConfig.JdbcDataSource j) {
            String q = applyWatermark(j.getQuery(), config);
            String ref = "sql-" + config.getPipeline().getName();
            camelContext.getRegistry().bind(ref, q);
            return "jdbc:etlDataSource?outputType=StreamList&query=#" + ref;
        }
        if (ds instanceof DataSourceConfig.KafkaDataSource k)
            return String.format("kafka:%s?brokers=%s&groupId=%s", k.getConnection().getTopic(),
                    k.getConnection().getBootstrapServers(), k.getConnection().getGroupId());
        if (ds instanceof DataSourceConfig.CsvDataSource c)
            return "file:" + c.getFilePath() + "?noop=true&charset=UTF-8";
        if (ds instanceof DataSourceConfig.SftpDataSource s)
            return String.format("sftp://%s@%s:%d%s?password=RAW(%s)&fileName=%s",
                    s.getConnection().getUsername(), s.getConnection().getHost(),
                    s.getConnection().getPort() > 0 ? s.getConnection().getPort() : 22,
                    s.getConnection().getDirectory(), s.getConnection().getPassword(),
                    s.getFileName() != null ? s.getFileName() : "*.*");
        throw new IllegalArgumentException("Unsupported datasource: " + ds.getType());
    }

    private String applyWatermark(String q, PipelineConfig c) {
        if (c.getWatermark() == null || c.getWatermark().getInitial() == null) return q;
        return q + " AND " + c.getWatermark().getColumn() + " >= '" + c.getWatermark().getInitial() + "'";
    }

    // ── Static helpers ────────────────────────────────────────
    private static String routeId(String name) { return "etl-" + name; }

    static Map<String, Object> aggregateRows(List<Map<String, Object>> rows,
                                               List<TransformDef.Aggregation> aggs,
                                               List<String> groupBy) {
        Map<String, Object> r = new LinkedHashMap<>();
        if (!groupBy.isEmpty() && !rows.isEmpty())
            for (String gb : groupBy) r.put(gb, rows.get(0).get(gb));
        for (var a : aggs) {
            String fn = a.getFunction().toUpperCase(), f = a.getField();
            String alias = a.getAlias() != null ? a.getAlias() : f;
            r.put(alias, switch (fn) {
                case "COUNT" -> (long) rows.size();
                case "SUM" -> rows.stream().mapToDouble(x -> toD(x.get(f))).sum();
                case "AVG" -> rows.stream().mapToDouble(x -> toD(x.get(f))).average().orElse(0);
                case "MIN" -> rows.stream().mapToDouble(x -> toD(x.get(f))).min().orElse(0);
                case "MAX" -> rows.stream().mapToDouble(x -> toD(x.get(f))).max().orElse(0);
                default -> 0L;
            });
        }
        return r;
    }

    private static double toD(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (NumberFormatException e) { return 0; }
    }

    static Object cast(Object v, String to) {
        if (v == null) return null;
        return switch (to.toUpperCase()) {
            case "STRING" -> v.toString();
            case "LONG" -> v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
            case "DOUBLE","DECIMAL" -> v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
            case "INT","INTEGER" -> v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
            case "BOOLEAN" -> v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
            default -> v;
        };
    }
}
