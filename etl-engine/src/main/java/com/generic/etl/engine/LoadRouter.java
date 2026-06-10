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
        Object rawBody = exchange.getIn().getBody();

        if (rawBody == null) {
            log.debug("Pipeline '{}': null body, skipping", pipelineName);
            return;
        }

        // Normalize body to List<Row> regardless of input format
        List<Row> rows = normalizeBody(rawBody, pipelineName);
        if (rows.isEmpty()) {
            log.debug("Pipeline '{}': empty body after normalization, skipping", pipelineName);
            return;
        }

        long startMs = System.currentTimeMillis();
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

    /**
     * Normalize any body type into List&lt;Row&gt;.
     * Handles: List&lt;Map&gt;, List&lt;Row&gt;, List&lt;List&gt; (Camel sql: stream),
     *          Map (single row), and raw List of anything else.
     */
    @SuppressWarnings("unchecked")
    private List<Row> normalizeBody(Object body, String pipelineName) {
        if (body instanceof List<?> list) {
            if (list.isEmpty()) return List.of();
            Object first = list.get(0);

            if (first instanceof Row) {
                return (List<Row>) list;
            }
            if (first instanceof Map) {
                return list.stream()
                        .map(m -> new Row(new LinkedHashMap<>((Map<String, Object>) m)))
                        .toList();
            }
            // Camel sql: component may return List<List> (StreamList mode) or raw column values
            if (first instanceof List) {
                log.debug("Pipeline '{}': body is List<List>, flattening", pipelineName);
                // Treat each inner list as a row with positional column names
                return list.stream()
                        .map(item -> {
                            Row row = new Row();
                            int i = 0;
                            for (Object val : (List<?>) item) {
                                row.put("col" + i++, val);
                            }
                            return row;
                        }).toList();
            }
            // Fallback: wrap each element as a single-column row
            log.warn("Pipeline '{}': unexpected body element type '{}', wrapping as _value column",
                    pipelineName, first.getClass().getSimpleName());
            return list.stream().map(item -> {
                Row row = new Row();
                row.put("_value", item);
                return row;
            }).toList();

        } else if (body instanceof Map<?, ?> m) {
            return List.of(new Row(new LinkedHashMap<>((Map<String, Object>) m)));
        } else {
            log.warn("Pipeline '{}': unexpected body type '{}', wrapping as single row",
                    pipelineName, body.getClass().getSimpleName());
            Row row = new Row();
            row.put("_value", body);
            return List.of(row);
        }
    }

    private String resolveOutputTable(String pipelineName) {
        var entries = lineageStore.getByPipeline(pipelineName);
        return entries.isEmpty() ? pipelineName
                : entries.get(entries.size() - 1).outputTable();
    }
}
