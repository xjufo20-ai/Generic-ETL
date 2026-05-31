package com.generic.etl.common.model;

import lombok.Data;

@Data
public class PersistConfig {
    private boolean enabled;
    private int threshold = 0;
    private StorageConfig storage;

    @Data
    public static class StorageConfig {
        private String type; // postgresql, clickhouse
        private String table;
        private ConnectionConfig connection;
    }
}
