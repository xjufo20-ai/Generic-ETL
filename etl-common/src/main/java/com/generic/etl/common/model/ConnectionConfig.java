package com.generic.etl.common.model;

import lombok.Data;

/**
 * Database connection configuration.
 * Shared by DataSourceConfig (source) and PersistConfig.StorageConfig (sink).
 */
@Data
public class ConnectionConfig {
    private String url;
    private String username;
    private String password;
    private String driverClass;
}
