package com.generic.etl.engine;

import com.generic.etl.core.store.AuditLog;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.engine.dispatch.ConsumerDispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class LoadRouter {
    private final ConsumerDispatchService dispatchService;
    private final ConsumerRegistry consumerRegistry;
    private final ResultCache inMemoryStore;
    private final AuditLog auditLog;

    /** Camel entry point — called from YAML DSL: to: bean:loadRouter */
    @SuppressWarnings("unchecked")
    public void route(Exchange exchange) {
        String pipelineName = exchange.getProperty("pipelineName", String.class);
        List<Map<String, Object>> body = exchange.getIn().getBody(List.class);
        if (body == null || body.isEmpty()) return;

        List<Row> rows = body.stream().map(m -> new Row(new LinkedHashMap<>(m))).toList();

        // Dispatch to consumers
        List<ConsumerRegistration> registrations = consumerRegistry.getByPipeline(pipelineName);
        if (!registrations.isEmpty()) {
            dispatchService.dispatch(pipelineName, rows, registrations);
        }

        // Stage in memory for PULL consumers
        inMemoryStore.put(pipelineName, rows);

        // Record execution audit
        auditLog.recordExecution(pipelineName, "SUCCESS", rows.size(), "system");

        log.info("Pipeline '{}': {} rows staged", pipelineName, rows.size());
    }
}
