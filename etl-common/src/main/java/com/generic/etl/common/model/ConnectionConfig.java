package com.generic.etl.common.model;

import lombok.Data;

/** Shared JDBC connection configuration, used by both DataSourceConfig and PersistConfig. */
@Data
public class ConnectionConfig {
    private String url;
    private String username;
    private String password;
    private String driverClass;
    private String type; // optional: oracle, mysql, postgresql
}
