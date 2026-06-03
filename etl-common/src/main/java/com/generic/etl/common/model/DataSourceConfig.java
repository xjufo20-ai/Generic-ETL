package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DataSourceConfig.JdbcDataSource.class, name = "oracle"),
    @JsonSubTypes.Type(value = DataSourceConfig.JdbcDataSource.class, name = "mysql"),
    @JsonSubTypes.Type(value = DataSourceConfig.JdbcDataSource.class, name = "postgresql"),
    @JsonSubTypes.Type(value = DataSourceConfig.CsvDataSource.class, name = "csv"),
    @JsonSubTypes.Type(value = DataSourceConfig.KafkaDataSource.class, name = "kafka"),
    @JsonSubTypes.Type(value = DataSourceConfig.SftpDataSource.class, name = "sftp")
})
public abstract class DataSourceConfig {
    protected String type;

    @Data @EqualsAndHashCode(callSuper = true)
    public static class JdbcDataSource extends DataSourceConfig {
        private ConnectionConfig connection; private String query; private CursorConfig cursor;
    }
    @Data @EqualsAndHashCode(callSuper = true)
    public static class CsvDataSource extends DataSourceConfig {
        private String filePath; private String delimiter = ","; private boolean hasHeader = true;
    }
    @Data @EqualsAndHashCode(callSuper = true)
    public static class KafkaDataSource extends DataSourceConfig { private KafkaConnection connection; }
    @Data @EqualsAndHashCode(callSuper = true)
    public static class SftpDataSource extends DataSourceConfig {
        private SftpConnection connection; private String fileName = "*.csv";
    }
    @Data public static class KafkaConnection { private String bootstrapServers; private String topic; private String groupId = "etl-group"; }
    @Data public static class SftpConnection { private String host; private int port = 22; private String username; private String password; private String directory = "/"; }
    @Data public static class CursorConfig { private String column; private int pageSize = 5000; }
}
