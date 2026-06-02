package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.core.config.PipelineConfigParser;
import com.generic.etl.core.transform.TransformPipeline;
import com.generic.etl.extract.adapter.ExtractorRegistry;
import com.generic.etl.load.LoadRouter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.nio.file.Path;

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
    @Bean public AuditLog auditLog(ObjectMapper mapper) { return new AuditLog(Path.of("data"), mapper); }

    @Bean
    public LineageStore lineageStore(ObjectMapper mapper) { return new LineageStore(Path.of("data"), mapper); }
    @Bean public StateStore stateStore(ObjectMapper mapper, AuditLog auditLog) { return new StateStore(Path.of("data"), mapper, auditLog); }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler(); s.setPoolSize(4); s.setThreadNamePrefix("etl-"); s.initialize();
        return s;
    }

    @Bean
    public PipelineExecutionService pipelineExecutionService(PipelineConfigParser configParser, TransformPipeline transformPipeline,
                                                               ExtractorRegistry extractorRegistry, LoadRouter loadRouter,
                                                               EtlMetrics metrics, AuditLog auditLog, LineageStore lineageStore) {
        return new PipelineExecutionService(configParser, transformPipeline, extractorRegistry, loadRouter, metrics, auditLog, lineageStore);
    }

    @Bean
    public PipelineScheduler pipelineScheduler(TaskScheduler taskScheduler, PipelineExecutionService executionService,
                                                PipelineConfigParser configParser, StateStore store) {
        return new PipelineScheduler(taskScheduler, executionService, configParser, store);
    }
}
