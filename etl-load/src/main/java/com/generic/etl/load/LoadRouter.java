package com.generic.etl.load;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.OutputConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadRouter {
    private static final Logger log = LoggerFactory.getLogger(LoadRouter.class);

    private final PersistHandler persistHandler;
    private final ConsumerDispatchService dispatchService;
    private final Map<String, List<ConsumerRegistration>> consumerRegistry;

    public LoadRouter(PersistHandler persistHandler,
                      ConsumerDispatchService dispatchService,
                      Map<String, List<ConsumerRegistration>> consumerRegistry) {
        this.persistHandler = persistHandler;
        this.dispatchService = dispatchService;
        this.consumerRegistry = consumerRegistry;
    }

    /**
     * Route transformed rows: persist if threshold met, dispatch to consumers.
     */
    public void route(String pipelineName, List<Row> rows, OutputConfig outputConfig) {
        if (rows.isEmpty()) {
            log.info("Pipeline '{}' produced 0 rows, skipping load", pipelineName);
            return;
        }

        // 1. Persist to intermediate storage if threshold met
        int persisted = persistHandler.persistIfNeeded(rows, outputConfig);
        log.info("Pipeline '{}': {} rows persisted of {} total", pipelineName, persisted, rows.size());

        // 2. Dispatch to registered consumers
        List<ConsumerRegistration> registrations = consumerRegistry.getOrDefault(pipelineName, List.of());
        if (!registrations.isEmpty()) {
            dispatchService.dispatch(pipelineName, rows, registrations);
        }

        // 3. Store in-memory for PULL consumers
        InMemoryDataStore.put(pipelineName, rows);
        log.info("Pipeline '{}': {} rows staged for PULL consumers", pipelineName, rows.size());
    }
}
