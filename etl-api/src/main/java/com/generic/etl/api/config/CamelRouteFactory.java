package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.load.LoadRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;

import java.util.*;

/**
 * Registers/unregisters PipelineConfig as Camel Routes with native EIP.
 * Delegates URI building to SourceUriBuilder, EIP mapping to TransformEipMapper.
 */
@Slf4j
public class CamelRouteFactory {

    private final CamelContext camelContext;
    private final PipelineConfigParser configParser;
    private final EtlMetrics metrics;
    private final AuditLog auditLog;
    private final LineageStore lineageStore;
    private final LoadRouter loadRouter;
    private final SourceUriBuilder uriBuilder;
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
        this.uriBuilder = new SourceUriBuilder(camelContext);
    }

    // ── Public API ────────────────────────────────────────────

    /** Register a pipeline as a Camel route with native EIP. */
    public synchronized void register(PipelineConfig config) throws Exception {
        String name = config.getPipeline().getName();
        String routeId = routeId(name);
        unregister(name);

        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                onException(Exception.class)
                    .handled(true).maximumRedeliveries(3).redeliveryDelay(5000)
                    .to("bean:camelDeadLetterHandler")
                    .log("FAILED: ${exchangeProperty.pipelineName} — ${exception.message}");

                from(uriBuilder.build(config))
                    .routeId(routeId)
                    .setProperty("pipelineName", constant(name))
                    .setProperty("startTime", simple("${date:now}"))
                    .process(e -> {
                        Object b = e.getIn().getBody();
                        if (!(b instanceof List)) e.getIn().setBody(b != null ? List.of(b) : List.of());
                    });

                // Apply transforms as native Camel EIP
                if (config.getTransforms() != null) {
                    for (var def : config.getTransforms()) {
                        TransformEipMapper.apply(this, def);
                    }
                }

                // Normalize body back to List<Map> for loadRouter
                .process(e -> {
                    Object b = e.getIn().getBody();
                    if (b instanceof Map) e.getIn().setBody(List.of(b));
                    else if (!(b instanceof List)) e.getIn().setBody(List.of());
                })

                // Load: multicast to persist + dispatch
                .multicast().parallelProcessing()
                    .to("bean:loadRouter?method=route")
                .end()

                // Metrics + Audit
                .process(e -> {
                    String pn = e.getProperty("pipelineName", String.class);
                    List<?> body = e.getIn().getBody(List.class);
                    int rows = body != null ? body.size() : 0;
                    long dur = System.currentTimeMillis() - e.getProperty("startTime", Long.class);
                    auditLog.recordExecution(pn, "SUCCESS", rows, "system");
                    metrics.recordSuccess(pn, rows, dur);
                    String tbl = config.getOutput() != null && config.getOutput().getStorage() != null
                            ? config.getOutput().getStorage().getTable() : "camel";
                    lineageStore.record(pn, tbl, "default", rows, "SUCCESS");
                })
                .log("OK: ${exchangeProperty.pipelineName} — ${body.size} rows");
            }
        });

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

    private static String routeId(String name) { return "etl-" + name; }
}
