package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.DataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DataSourceManager {
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    public Connection getConnection(DataSourceConfig.ConnectionConfig config) {
        String key = config.getUrl() + "|" + config.getUsername();
        HikariDataSource ds = dataSources.computeIfAbsent(key, k -> createPool(config));
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get JDBC connection", e);
        }
    }

    /**
     * Create a standalone DataSource (for Camel registry binding).
     */
    public static DataSource createDataSource(DataSourceConfig.ConnectionConfig config) {
        return createPool(config);
    }

    private static HikariDataSource createPool(DataSourceConfig.ConnectionConfig config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setMaximumPoolSize(10);
        hc.setMinimumIdle(2);
        hc.setConnectionTimeout(10000);
        hc.setIdleTimeout(300000);
        hc.setMaxLifetime(600000);
        if (config.getDriverClass() != null) {
            hc.setDriverClassName(config.getDriverClass());
        }
        log.info("Created connection pool for {}", config.getUrl());
        return new HikariDataSource(hc);
    }

    public void shutdown() {
        dataSources.values().forEach(HikariDataSource::close);
        dataSources.clear();
    }
}
