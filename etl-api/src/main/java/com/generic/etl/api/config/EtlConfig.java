package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;
import com.generic.etl.core.transform.TransformProcessor;
import com.generic.etl.extract.adapter.*;
import com.generic.etl.extract.adapter.impl.*;
import com.generic.etl.load.LoadRouter;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import com.generic.etl.load.persist.PersistHandler;
import com.generic.etl.transform.impl.AggregateProcessor;
import com.generic.etl.transform.impl.FilterProcessor;
import com.generic.etl.transform.impl.JoinProcessor;
import com.generic.etl.transform.impl.RenameProcessor;
import com.generic.etl.transform.impl.TypeCastProcessor;
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

    // ── Extraction ────────────────────────────────────────

    @Bean
    public DataSourceManager dataSourceManager() {
        return new DataSourceManager();
    }

    @Bean
    public JdbcExtractor jdbcExtractor(DataSourceManager dsManager) {
        return new JdbcExtractor(dsManager);
    }

    @Bean
    public CsvExtractor csvExtractor() {
        return new CsvExtractor();
    }

    @Bean
    public ExtractorRegistry extractorRegistry(JdbcExtractor jdbc, CsvExtractor csv) {
        return new ExtractorRegistry(List.of(jdbc, csv));
    }

    // ── Transform ─────────────────────────────────────────

    @Bean
    public FilterProcessor filterProcessor() { return new FilterProcessor(); }

    @Bean
    public RenameProcessor renameProcessor() { return new RenameProcessor(); }

    @Bean
    public TypeCastProcessor typeCastProcessor() { return new TypeCastProcessor(); }

    @Bean
    public AggregateProcessor aggregateProcessor() { return new AggregateProcessor(); }

    @Bean
    public JoinProcessor joinProcessor(DataSource dataSource) { return new JoinProcessor(dataSource); }

    @Bean
    public TransformChain transformChain(List<TransformProcessor> processors) {
        Map<String, TransformProcessor> map = new LinkedHashMap<>();
        map.put("filter", find(processors, FilterProcessor.class));
        map.put("rename", find(processors, RenameProcessor.class));
        map.put("typeCast", find(processors, TypeCastProcessor.class));
        map.put("aggregate", find(processors, AggregateProcessor.class));
        map.put("join", find(processors, JoinProcessor.class));
        return new TransformChain(map);
    }

    // ── Load ──────────────────────────────────────────────

    @Bean
    public PersistHandler persistHandler(DataSource dataSource) { return new PersistHandler(dataSource); }

    @Bean
    public ConsumerDispatchService consumerDispatchService() { return new ConsumerDispatchService(); }

    @Bean
    public Map<String, List<ConsumerRegistration>> consumerRegistry() { return new ConcurrentHashMap<>(); }

    @Bean
    public Map<String, String> pipelineStore() { return new ConcurrentHashMap<>(); }

    @Bean
    public LoadRouter loadRouter(PersistHandler persistHandler,
                                  ConsumerDispatchService dispatchService,
                                  Map<String, List<ConsumerRegistration>> consumerRegistry) {
        return new LoadRouter(persistHandler, dispatchService, consumerRegistry);
    }

    // ── Orchestration ─────────────────────────────────────

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(4);
        s.setThreadNamePrefix("etl-scheduler-");
        s.initialize();
        return s;
    }

    @Bean
    public PipelineExecutionService pipelineExecutionService(PipelineConfigParser configParser,
                                                               TransformChain transformChain,
                                                               ExtractorRegistry extractorRegistry,
                                                               LoadRouter loadRouter) {
        return new PipelineExecutionService(configParser, transformChain, extractorRegistry, loadRouter);
    }

    @Bean
    public PipelineScheduler pipelineScheduler(TaskScheduler taskScheduler,
                                                PipelineExecutionService executionService,
                                                PipelineConfigParser configParser,
                                                Map<String, String> pipelineStore) {
        return new PipelineScheduler(taskScheduler, executionService, configParser, pipelineStore);
    }

    private TransformProcessor find(List<TransformProcessor> list, Class<?> type) {
        return list.stream().filter(p -> type.isAssignableFrom(p.getClass())).findFirst()
                .orElseThrow(() -> new IllegalStateException("Missing processor: " + type.getSimpleName()));
    }
}
