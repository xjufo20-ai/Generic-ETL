package com.generic.etl.api;

import com.generic.etl.load.ResultCache;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
@EnableDiscoveryClient
public class EtlApplication {

    private final ResultCache inMemoryDataStore;

    public EtlApplication(ResultCache inMemoryDataStore) {
        this.inMemoryDataStore = inMemoryDataStore;
    }

    public static void main(String[] args) {
        SpringApplication.run(EtlApplication.class, args);
    }

    @Scheduled(fixedRate = 60000)
    public void evictExpiredCache() { inMemoryDataStore.evictExpired(); }
}
