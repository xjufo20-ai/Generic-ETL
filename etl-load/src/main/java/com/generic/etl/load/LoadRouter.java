package com.generic.etl.load;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.PersistConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class LoadRouter {

    private final PersistHandler persistHandler;
    private final ConsumerDispatchService dispatchService;
    private final ConsumerRegistry consumerRegistry;
    private final InMemoryDataStore inMemoryStore;

    public LoadRouter(PersistHandler persistHandler,
                      ConsumerDispatchService dispatchService,
                      ConsumerRegistry consumerRegistry,
                      InMemoryDataStore inMemoryStore) {
        this.persistHandler = persistHandler;
        this.dispatchService = dispatchService;
        this.consumerRegistry = consumerRegistry;
        this.inMemoryStore = inMemoryStore;
    }

    public void route(String pipelineName, List<Row> rows, PersistConfig persistConfig) {
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
