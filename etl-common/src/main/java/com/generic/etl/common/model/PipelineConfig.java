package com.generic.etl.common.model;

import lombok.Data;
import java.util.List;

@Data
public class PipelineConfig {
    private Pipeline pipeline;
    private DataSourceConfig datasource;
    private SchemaConfig inputSchema;
    private List<TransformDef> transforms;
    private SchemaConfig outputSchema;
    private WatermarkConfig watermark;
    private PersistConfig output;

    @Data public static class Pipeline {
        private String name;
        private String version;
        private String cron;
    }
}
