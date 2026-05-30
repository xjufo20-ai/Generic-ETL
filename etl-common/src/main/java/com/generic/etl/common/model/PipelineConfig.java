package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
public class PipelineConfig {
    private Pipeline pipeline;
    private DataSourceConfig datasource;
    private SchemaConfig inputSchema;
    private List<TransformDef> transforms;
    private PersistConfig output;

    @Data
    public static class Pipeline {
        private String name;
        private String version;
        private String cron;
    }
}
