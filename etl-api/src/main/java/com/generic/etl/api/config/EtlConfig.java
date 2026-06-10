package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.core.store.AuditLog;
import com.generic.etl.core.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.nio.file.Path;

@Configuration
public class EtlConfig {

    private static final Path DATA_DIR = Path.of("data");

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Bean
    public AuditLog auditLog(ObjectMapper mapper) {
        return new AuditLog(DATA_DIR, mapper);
    }

    @Bean
    public LineageStore lineageStore(ObjectMapper mapper) {
        return new LineageStore(DATA_DIR, mapper);
    }

    @Bean
    public StateStore stateStore(ObjectMapper mapper) {
        return new StateStore(DATA_DIR, mapper);
    }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("etl-");
        scheduler.initialize();
        return scheduler;
    }
}
