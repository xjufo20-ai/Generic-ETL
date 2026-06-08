package com.generic.etl.api.config;

import com.generic.etl.core.store.AuditLog;
import com.generic.etl.engine.ConsumerRegistry;
import com.generic.etl.engine.LoadRouter;
import com.generic.etl.engine.ResultCache;
import com.generic.etl.engine.dispatch.ConsumerDispatchService;
import com.generic.etl.engine.persist.PersistHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;

@Configuration
public class LoadConfig {

    @Bean public PersistHandler persistHandler(DataSource dataSource) { return new PersistHandler(dataSource); }
    @Bean public ConsumerRegistry consumerRegistry() { return new ConsumerRegistry(); }
    @Bean public ResultCache resultCache() { return new ResultCache(); }

    @Bean
    public ConsumerDispatchService consumerDispatchService(RestClient.Builder builder) {
        return new ConsumerDispatchService(builder);
    }

    @Bean
    public LoadRouter loadRouter(ConsumerDispatchService dispatchService, ConsumerRegistry consumerRegistry,
                                  ResultCache resultCache, AuditLog auditLog) {
        return new LoadRouter(dispatchService, consumerRegistry, resultCache, auditLog);
    }
}
