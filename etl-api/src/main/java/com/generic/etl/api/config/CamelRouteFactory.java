package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.common.model.*;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.load.LoadRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;

import java.util.*;

/**
 * Maps PipelineConfig → Camel Route dynamically.
 * Each pipeline becomes a Camel route: from(source) → transforms → multicast(persist + dispatch).
 *
 * Usage:
 *   factory.register(config);   // add/update route
 *   factory.remove("name");     // stop and remove route
 *   factory.execute("name");    // trigger route via direct: endpoint
 */
@Slf4j
public class CamelRouteFactory {

    private final CamelContext camelContext;
    private final PipelineConfigParser configParser;
    private final EtlMetrics metrics;
    private final AuditLog auditLog;
    private final LineageStore lineageStore;
    private final LoadRouter loadRouter;

    /** Set of registered pipeline names (for idempotent register). */
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

    /**
     * Register (or update) a pipeline as a Camel route.
     * Idempotent: stops and removes any existing route with the same name first.
     */
    public synchronized void register(PipelineConfig config) throws Exception {
        String name = config.getPipeline().getName();
        String routeId = routeId(name);

        // Remove existing route if present
        unregister(name);

        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // ── Global error handling ──
                onException(Exception.class)
                    .handled(true)
                    .maximumRedeliveries(3)
                    .redeliveryDelay(5000)
                    .to("bean:deadLetterHandler?method=handle");

                // ── Main pipeline route ──
                from(buildSourceUri(config))
                    .routeId(routeId)
                    .setProperty("pipelineName", constant(name))
                    .setProperty("startTime", simple("${date:now}"))
                    // Transforms
                    .pipeline("etl-transform-" + name)  // allow parallel processing inside
                    // Persist + Dispatch via multicast
                    .multicast().parallelProcessing()
                        .to("bean:loadRouter?method=route")
                    .end()
                    // Metrics + Audit (wireTap so error in metrics doesn't break pipeline)
                    .wireTap("direct:etl-metrics")
                    .to("bean:auditLog?method=recordExecution");
            }
        });

        registeredPipelines.add(name);
        log.info("Registered Camel route: {} → {}", routeId, buildSourceUri(config));
    }

    /** Remove a pipeline route. Safe to call if not registered. */
    public synchronized void unregister(String name) {
        String routeId = routeId(name);
        try {
            if (camelContext.getRoute(routeId) != null) {
                camelContext.getRouteController().stopRoute(routeId);
                camelContext.removeRoute(routeId);
                log.info("Removed Camel route: {}", routeId);
            }
        } catch (Exception e) {
            log.warn("Failed to unregister route {}: {}", routeId, e.getMessage());
        }
        registeredPipelines.remove(name);
    }

    /** Trigger a registered pipeline immediately via direct: endpoint. */
    public void execute(String name) {
        String uri = "direct:trigger-" + name;
        camelContext.createProducerTemplate().sendBody(uri, null);
    }

    /** Check if a pipeline is registered. */
    public boolean isRegistered(String name) {
        return registeredPipelines.contains(name);
    }

    // ── URI Builders ──────────────────────────────────────────

    String buildSourceUri(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();

        if (ds instanceof DataSourceConfig.JdbcDataSource j) {
            String query = j.getQuery();
            if (config.getWatermark() != null) {
                query = applyWatermark(query, config);
            }
            // Register query as a named constant in Camel registry
            String queryRef = "sql-" + config.getPipeline().getName();
            camelContext.getRegistry().bind(queryRef, query);
            return "jdbc:etlDataSource?outputType=StreamList&query=#" + queryRef;
        }

        if (ds instanceof DataSourceConfig.KafkaDataSource k) {
            return String.format("kafka:%s?brokers=%s&groupId=%s",
                    k.getConnection().getTopic(),
                    k.getConnection().getBootstrapServers(),
                    k.getConnection().getGroupId());
        }

        if (ds instanceof DataSourceConfig.CsvDataSource c) {
            return String.format("file:%s?noop=true&charset=UTF-8", c.getFilePath());
        }

        if (ds instanceof DataSourceConfig.SftpDataSource s) {
            return String.format("sftp://%s@%s:%d%s?password=RAW(%s)&fileName=%s",
                    s.getConnection().getUsername(),
                    s.getConnection().getHost(),
                    s.getConnection().getPort() > 0 ? s.getConnection().getPort() : 22,
                    s.getConnection().getDirectory(),
                    s.getConnection().getPassword(),
                    s.getFileName() != null ? s.getFileName() : "*.*");
        }

        throw new IllegalArgumentException("Unsupported datasource type: " + ds.getType());
    }

    /** Apply watermark filter to SQL query. */
    private String applyWatermark(String query, PipelineConfig config) {
        WatermarkConfig wm = config.getWatermark();
        if (wm == null) return query;
        // Watermark value is stored in StateStore; for now, append a placeholder
        String col = wm.getColumn();
        if (wm.getInitial() != null) {
            return query + " AND " + col + " >= '" + wm.getInitial() + "'";
        }
        return query;
    }

    // ── Helpers ────────────────────────────────────────────────

    private static String routeId(String name) {
        return "etl-" + name;
    }
}
