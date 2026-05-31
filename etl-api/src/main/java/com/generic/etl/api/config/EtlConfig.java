package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformChain;
import com.generic.etl.extract.adapter.ExtractorRegistry;
import com.generic.etl.load.LoadRouter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Orchestration beans: scheduler, execution service, pipeline store. */
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

    @Bean public PipelineConfigParser pipelineConfigParser(ObjectMapper mapper) { return new PipelineConfigParser(mapper); }
    @Bean public Map<String, String> pipelineStore() { return new ConcurrentHashMap<>(); }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(4); s.setThreadNamePrefix("etl-scheduler-"); s.initialize();
        return s;
    }

    @Bean
    public PipelineExecutionService pipelineExecutionService(PipelineConfigParser configParser,
                                                               TransformChain transformChain,
                                                               ExtractorRegistry extractorRegistry,
                                                               LoadRouter loadRouter, EtlMetrics metrics) {
        return new PipelineExecutionService(configParser, transformChain, extractorRegistry, loadRouter, metrics);
    }

    @Bean
    public PipelineScheduler pipelineScheduler(TaskScheduler taskScheduler, PipelineExecutionService executionService,
                                                PipelineConfigParser configParser, Map<String, String> pipelineStore) {
        return new PipelineScheduler(taskScheduler, executionService, configParser, pipelineStore);
    }
}
