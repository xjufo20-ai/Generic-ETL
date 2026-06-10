package com.generic.etl.engine;

import com.generic.etl.core.metrics.MetricsRecorder;
import com.generic.etl.core.store.AuditLog;
import com.generic.etl.core.store.LineageStore;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.engine.dispatch.ConsumerDispatchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class LoadRouter {
    private final ConsumerDispatchService dispatchService;
    private final ConsumerRegistry consumerRegistry;
    private final ResultCache inMemoryStore;
    private final AuditLog auditLog;
    private final MetricsRecorder metrics;
    private final LineageStore lineageStore;

    public LoadRouter(ConsumerDispatchService dispatchService, ConsumerRegistry consumerRegistry,
                      ResultCache resultCache, AuditLog auditLog,
                      MetricsRecorder metrics, LineageStore lineageStore) {
        this.dispatchService = dispatchService;
        this.consumerRegistry = consumerRegistry;
        this.inMemoryStore = resultCache;
        this.auditLog = auditLog;
        this.metrics = metrics;
        this.lineageStore = lineageStore;
    }

    /** Camel entry point — called from YAML DSL: to: bean:loadRouter */
    @SuppressWarnings("unchecked")
    public void route(Exchange exchange) {
        String pipelineName = exchange.getProperty("pipelineName", String.class);
        List<Map<String, Object>> body = exchange.getIn().getBody(List.class);
        if (body == null || body.isEmpty()) {
            log.debug("Pipeline '{}': empty body, skipping", pipelineName);
            return;
        }

        long startMs = System.currentTimeMillis();
        List<Row> rows = body.stream().map(m -> new Row(new LinkedHashMap<>(m))).toList();
        int rowCount = rows.size();

        // Dispatch to consumers
        List<ConsumerRegistration> registrations = consumerRegistry.getByPipeline(pipelineName);
        if (!registrations.isEmpty()) {
            dispatchService.dispatch(pipelineName, rows, registrations);
        }

        // Stage in memory for PULL consumers
        inMemoryStore.put(pipelineName, rows);

        // Record execution audit
        auditLog.recordExecution(pipelineName, "SUCCESS", rowCount, "system");

        // Record data lineage
        String outputTable = resolveOutputTable(pipelineName);
        for (ConsumerRegistration reg : registrations) {
            lineageStore.record(pipelineName, outputTable,
                    reg.getConsumer().getName(), rowCount, "SUCCESS");
        }
        if (registrations.isEmpty()) {
            lineageStore.record(pipelineName, outputTable, null, rowCount, "SUCCESS");
        }

        // Record metrics
        long durationMs = System.currentTimeMillis() - startMs;
        metrics.recordSuccess(pipelineName, rowCount, durationMs);

        log.info("Pipeline '{}': {} rows staged in {}ms", pipelineName, rowCount, durationMs);
    }

    private String resolveOutputTable(String pipelineName) {
        var entries = lineageStore.getByPipeline(pipelineName);
        return entries.isEmpty() ? pipelineName
                : entries.get(entries.size() - 1).outputTable();
    }
}
