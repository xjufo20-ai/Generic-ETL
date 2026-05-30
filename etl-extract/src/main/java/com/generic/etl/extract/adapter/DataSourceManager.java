package com.generic.etl.extract.adapter;

import com.generic.etl.common.model.DataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceManager {
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    public Connection getConnection(DataSourceConfig.ConnectionConfig config) {
        String key = config.getUrl() + "|" + config.getUsername();
        HikariDataSource ds = dataSources.computeIfAbsent(key, k -> createDataSource(config));
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get JDBC connection", e);
        }
    }

    private HikariDataSource createDataSource(DataSourceConfig.ConnectionConfig config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setMaximumPoolSize(5);
        hc.setMinimumIdle(1);
        hc.setConnectionTimeout(10000);
        hc.setIdleTimeout(300000);
        hc.setMaxLifetime(600000);
        if (config.getDriverClass() != null) {
            hc.setDriverClassName(config.getDriverClass());
        }
        return new HikariDataSource(hc);
    }

    public void shutdown() {
        dataSources.values().forEach(HikariDataSource::close);
        dataSources.clear();
    }
}
