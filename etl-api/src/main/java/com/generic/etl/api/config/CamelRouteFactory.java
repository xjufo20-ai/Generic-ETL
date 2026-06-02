package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.common.model.*;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.CamelTransformAdapter;
import com.generic.etl.core.transform.TransformProcessor;
import com.generic.etl.load.LoadRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;

import java.util.*;

/**
 * Maps PipelineConfig → Camel Route dynamically.
 * Each pipeline becomes: from(source) → transform → multicast(persist + dispatch).
 */
@Slf4j
public class CamelRouteFactory {

    private final CamelContext camelContext;
    private final PipelineConfigParser configParser;
    private final EtlMetrics metrics;
    private final AuditLog auditLog;
    private final LineageStore lineageStore;
    private final LoadRouter loadRouter;
    private final Map<String, TransformProcessor> transformProcessors;
    private final Set<String> registeredPipelines = new HashSet<>();

    public CamelRouteFactory(CamelContext camelContext, PipelineConfigParser configParser,
                              EtlMetrics metrics, AuditLog auditLog, LineageStore lineageStore,
                              LoadRouter loadRouter, Map<String, TransformProcessor> transformProcessors) {
        this.camelContext = camelContext;
        this.configParser = configParser;
        this.metrics = metrics;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
        this.loadRouter = loadRouter;
        this.transformProcessors = transformProcessors;
    }

    // ── Public API ────────────────────────────────────────────

    /** Register (or update) a pipeline as a Camel route. */
    public synchronized void register(PipelineConfig config) throws Exception {
        String name = config.getPipeline().getName();
        String routeId = routeId(name);
        unregister(name);

        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                onException(Exception.class)
                    .handled(true)
                    .maximumRedeliveries(3)
                    .redeliveryDelay(5000)
                    .log("Pipeline '${exchangeProperty.pipelineName}' failed: ${exception.message}");

                from(buildSourceUri(config))
                    .routeId(routeId)
                    .setProperty("pipelineName", constant(name))
                    // Apply transforms (if any)
                    .process(buildTransformSteps(config))
                    // Multicast: persist + dispatch in parallel
                    .multicast().parallelProcessing()
                        .to("bean:loadRouter?method=route")
                    .end()
                    .to("bean:auditLog?method=recordExecution")
                    .log("Pipeline '${exchangeProperty.pipelineName}' completed");
            }
        });

        registeredPipelines.add(name);
        log.info("Registered Camel route: {} → {}", routeId, buildSourceUri(config));
    }

    /** Remove a pipeline route. */
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

    /** Trigger a registered pipeline immediately. */
    public void execute(String name) {
        camelContext.createProducerTemplate().sendBody("direct:trigger-" + name, null);
    }

    public boolean isRegistered(String name) { return registeredPipelines.contains(name); }
    public Set<String> getRegisteredPipelines() { return Collections.unmodifiableSet(registeredPipelines); }

    // ── URI / Step Builders ───────────────────────────────────

    String buildSourceUri(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();

        if (ds instanceof DataSourceConfig.JdbcDataSource j) {
            String query = j.getQuery();
            if (config.getWatermark() != null) {
                query = applyWatermark(query, config);
            }
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

    private org.apache.camel.Processor buildTransformSteps(PipelineConfig config) {
        if (config.getTransforms() == null || config.getTransforms().isEmpty()) {
            return exchange -> {}; // no-op
        }
        return CamelTransformAdapter.of(transformProcessors, config.getTransforms());
    }

    private String applyWatermark(String query, PipelineConfig config) {
        WatermarkConfig wm = config.getWatermark();
        if (wm == null) return query;
        if (wm.getInitial() != null) {
            return query + " AND " + wm.getColumn() + " >= '" + wm.getInitial() + "'";
        }
        return query;
    }

    private static String routeId(String name) { return "etl-" + name; }
}
