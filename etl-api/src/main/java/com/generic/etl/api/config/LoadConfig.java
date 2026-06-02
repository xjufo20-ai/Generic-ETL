package com.generic.etl.api.config;

import com.generic.etl.api.store.StateStore;
import com.generic.etl.load.ConsumerRegistry;
import com.generic.etl.load.ResultCache;
import com.generic.etl.load.LoadRouter;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class LoadConfig {

    @Bean public PersistHandler persistHandler(DataSource dataSource) { return new PersistHandler(dataSource); }
    @Bean public ConsumerDispatchService consumerDispatchService() { return new ConsumerDispatchService(); }

    @Bean
    public ConsumerRegistry consumerRegistry(StateStore store) {
        ConsumerRegistry registry = new ConsumerRegistry();
        store.getAllConsumers().forEach(registry::register);
        return registry;
    }

    @Bean public ResultCache inMemoryDataStore() { return new ResultCache(3600, 100_000); }

    @Bean
    public LoadRouter loadRouter(PersistHandler persistHandler, ConsumerDispatchService dispatchService,
                                  ConsumerRegistry consumerRegistry, ResultCache inMemoryDataStore) {
        return new LoadRouter(persistHandler, dispatchService, consumerRegistry, inMemoryDataStore);
    }
}
