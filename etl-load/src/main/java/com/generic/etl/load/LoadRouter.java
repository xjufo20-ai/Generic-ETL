package com.generic.etl.load;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.PersistConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public class LoadRouter {

    private final PersistHandler persistHandler;
    private final ConsumerDispatchService dispatchService;
    private final ConsumerRegistry consumerRegistry;
    private final ResultCache inMemoryStore;

    public LoadRouter(PersistHandler persistHandler,
                      ConsumerDispatchService dispatchService,
                      ConsumerRegistry consumerRegistry,
                      ResultCache inMemoryStore) {
        this.persistHandler = persistHandler;
        this.dispatchService = dispatchService;
        this.consumerRegistry = consumerRegistry;
        this.inMemoryStore = inMemoryStore;
    }

    /** Java pipeline entry point. */
    public void route(String pipelineName, List<Row> rows, PersistConfig persistConfig) {
        doRoute(pipelineName, rows, persistConfig);
    }

    /** Camel route entry point — extracts pipeline name and rows from Exchange. */
    public void route(Exchange exchange) {
        String pipelineName = exchange.getProperty("pipelineName", String.class);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> body = exchange.getIn().getBody(List.class);

        if (body == null || body.isEmpty()) {
            log.info("Pipeline '{}' produced 0 rows, skipping load", pipelineName);
            return;
        }

        List<Row> rows = body.stream()
                .map(m -> new Row(new LinkedHashMap<>(m)))
                .collect(Collectors.toList());

        // PersistConfig is null for Camel path (output configured via route endpoint)
        doRoute(pipelineName, rows, null);
    }

    private void doRoute(String pipelineName, List<Row> rows, PersistConfig persistConfig) {
        if (rows.isEmpty()) {
            log.info("Pipeline '{}' produced 0 rows, skipping load", pipelineName);
            return;
        }

        int persisted = persistHandler.persistIfNeeded(rows, persistConfig);
        log.info("Pipeline '{}': {} rows persisted of {} total", pipelineName, persisted, rows.size());

        List<ConsumerRegistration> registrations = consumerRegistry.getByPipeline(pipelineName);
        if (!registrations.isEmpty()) {
            dispatchService.dispatch(pipelineName, rows, registrations);
        }

        inMemoryStore.put(pipelineName, rows);
        log.info("Pipeline '{}': {} rows staged for PULL consumers", pipelineName, rows.size());
    }
}
