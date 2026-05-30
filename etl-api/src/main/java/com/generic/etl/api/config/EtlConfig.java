package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;
import com.generic.etl.core.transform.TransformProcessor;
import com.generic.etl.extract.adapter.CamelExtractAdapter;
import com.generic.etl.extract.adapter.DataSourceManager;
import com.generic.etl.load.LoadRouter;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import com.generic.etl.transform.aggregate.AggregateProcessor;
import com.generic.etl.transform.filter.FilterProcessor;
import com.generic.etl.transform.join.JoinProcessor;
import com.generic.etl.transform.rename.RenameProcessor;
import com.generic.etl.transform.typecast.TypeCastProcessor;
import org.apache.camel.CamelContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class EtlConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }

    @Bean
    public PipelineConfigParser pipelineConfigParser(ObjectMapper mapper) {
        return new PipelineConfigParser(mapper);
    }

    @Bean
    public DataSourceManager dataSourceManager() {
        return new DataSourceManager();
    }

    /** Camel-based extraction — single adapter handles all datasource types. */
    @Bean
    public CamelExtractAdapter camelExtractAdapter(CamelContext camelContext) {
        return new CamelExtractAdapter(camelContext);
    }

    @Bean
    public FilterProcessor filterProcessor() {
        return new FilterProcessor();
    }

    @Bean
    public RenameProcessor renameProcessor() {
        return new RenameProcessor();
    }

    @Bean
    public TypeCastProcessor typeCastProcessor() {
        return new TypeCastProcessor();
    }

    @Bean
    public AggregateProcessor aggregateProcessor() {
        return new AggregateProcessor();
    }

    @Bean
    public JoinProcessor joinProcessor(DataSource dataSource) {
        return new JoinProcessor(dataSource);
    }

    @Bean
    public TransformChain transformChain(List<TransformProcessor> processors) {
        Map<String, TransformProcessor> map = new LinkedHashMap<>();
        map.put("filter", findProcessor(processors, FilterProcessor.class));
        map.put("rename", findProcessor(processors, RenameProcessor.class));
        map.put("typeCast", findProcessor(processors, TypeCastProcessor.class));
        map.put("aggregate", findProcessor(processors, AggregateProcessor.class));
        map.put("join", findProcessor(processors, JoinProcessor.class));
        return new TransformChain(map);
    }

    @Bean
    public PersistHandler persistHandler(DataSource dataSource) {
        return new PersistHandler(dataSource);
    }

    @Bean
    public ConsumerDispatchService consumerDispatchService() {
        return new ConsumerDispatchService();
    }

    @Bean
    public Map<String, List<ConsumerRegistration>> consumerRegistry() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public Map<String, String> pipelineStore() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("etl-scheduler-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    public LoadRouter loadRouter(PersistHandler persistHandler,
                                  ConsumerDispatchService dispatchService,
                                  Map<String, List<ConsumerRegistration>> consumerRegistry) {
        return new LoadRouter(persistHandler, dispatchService, consumerRegistry);
    }

    @Bean
    public PipelineExecutionService pipelineExecutionService(PipelineConfigParser configParser,
                                                               TransformChain transformChain,
                                                               CamelExtractAdapter extractAdapter,
                                                               LoadRouter loadRouter) {
        return new PipelineExecutionService(configParser, transformChain, extractAdapter, loadRouter);
    }

    @Bean
    public PipelineScheduler pipelineScheduler(TaskScheduler taskScheduler,
                                                PipelineExecutionService executionService,
                                                PipelineConfigParser configParser,
                                                Map<String, String> pipelineStore) {
        return new PipelineScheduler(taskScheduler, executionService, configParser, pipelineStore);
    }

    private TransformProcessor findProcessor(List<TransformProcessor> processors, Class<?> type) {
        return processors.stream()
                .filter(p -> type.isAssignableFrom(p.getClass()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No processor found for " + type.getSimpleName()));
    }
}
