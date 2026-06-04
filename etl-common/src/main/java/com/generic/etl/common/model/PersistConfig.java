package com.generic.etl.common.model;
import lombok.Data;
import java.util.List;
@Data
public class PersistConfig {
    private boolean enabled = true; private int threshold;
    private StorageConfig storage;
    @Data public static class StorageConfig {
        private String type; private String table; private List<String> primaryKeys; private String mode = "insert";
        /** Optional: per-pipeline DB connection. When set, overrides the default DataSource. */
        private ConnectionConfig connection;
    }
}
