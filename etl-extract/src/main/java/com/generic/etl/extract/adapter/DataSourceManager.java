package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.ConnectionConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DataSourceManager {
    private final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    public DataSource getOrCreate(ConnectionConfig config) {
        String key = config.getUrl() + "|" + config.getUsername();
        return pools.computeIfAbsent(key, k -> createPool(config));
    }

    private HikariDataSource createPool(ConnectionConfig config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setMaximumPoolSize(10);
        hc.setMinimumIdle(2);
        hc.setConnectionTimeout(10000);
        hc.setIdleTimeout(300000);
        hc.setMaxLifetime(600000);
        if (config.getDriverClass() != null) hc.setDriverClassName(config.getDriverClass());
        log.info("Created pool for {}", config.getUrl());
        return new HikariDataSource(hc);
    }

    public void shutdown() { pools.values().forEach(HikariDataSource::close); pools.clear(); }
}
