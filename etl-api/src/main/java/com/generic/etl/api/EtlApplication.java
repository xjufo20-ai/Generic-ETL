package com.generic.etl.api;

import com.generic.etl.engine.ResultCache;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@ComponentScan(basePackages = "com.generic.etl")
@EnableScheduling
@RequiredArgsConstructor
public class EtlApplication {
    private final ResultCache resultCache;

    public static void main(String[] args) {
        SpringApplication.run(EtlApplication.class, args);
    }

    @Scheduled(fixedRate = 60000)
    public void evictExpiredCache() { resultCache.evictExpired(); }
}
