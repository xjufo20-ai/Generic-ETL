package com.generic.etl.api.config;

import com.generic.etl.load.ConsumerRegistry;
import com.generic.etl.load.LoadRouter;
import com.generic.etl.load.ResultCache;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class LoadConfig {

    @Bean public PersistHandler persistHandler(DataSource dataSource) { return new PersistHandler(dataSource); }
    @Bean public ConsumerRegistry consumerRegistry() { return new ConsumerRegistry(); }
    @Bean public ResultCache resultCache() { return new ResultCache(); }
    @Bean public ConsumerDispatchService consumerDispatchService() { return new ConsumerDispatchService(); }

    @Bean
    public LoadRouter loadRouter(PersistHandler persistHandler, ConsumerDispatchService dispatchService,
                                  ConsumerRegistry consumerRegistry, ResultCache resultCache) {
        return new LoadRouter(persistHandler, dispatchService, consumerRegistry, resultCache);
    }
}
