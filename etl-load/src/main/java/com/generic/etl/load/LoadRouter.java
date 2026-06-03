package com.generic.etl.load;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class LoadRouter {
    private final PersistHandler persistHandler;
    private final ConsumerDispatchService dispatchService;
    private final ConsumerRegistry consumerRegistry;
    private final ResultCache inMemoryStore;

    public LoadRouter(PersistHandler persistHandler, ConsumerDispatchService dispatchService,
                      ConsumerRegistry consumerRegistry, ResultCache inMemoryStore) {
        this.persistHandler = persistHandler;
        this.dispatchService = dispatchService;
        this.consumerRegistry = consumerRegistry;
        this.inMemoryStore = inMemoryStore;
    }

    /** Camel entry point — called from YAML DSL: to: bean:loadRouter */
    public void route(Exchange exchange) {
        String pipelineName = exchange.getProperty("pipelineName", String.class);
        @SuppressWarnings("unchecked")
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
        log.info("Pipeline '{}': {} rows staged", pipelineName, rows.size());
    }
}
