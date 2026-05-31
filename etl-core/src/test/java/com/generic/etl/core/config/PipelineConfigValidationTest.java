package com.generic.etl.core.config;

import com.generic.etl.common.model.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigValidationTest {

    @Test
    void shouldFailOnMissingPipelineName() {
        PipelineConfig config = new PipelineConfig();
        config.setPipeline(new PipelineConfig.Pipeline());
        List<String> issues = config.validate();
        assertTrue(issues.stream().anyMatch(i -> i.contains("pipeline.name")));
    }

    @Test
    void shouldFailOnMissingDatasource() {
        PipelineConfig config = new PipelineConfig();
        PipelineConfig.Pipeline p = new PipelineConfig.Pipeline(); p.setName("test");
        config.setPipeline(p);
        List<String> issues = config.validate();
        assertTrue(issues.stream().anyMatch(i -> i.contains("datasource")));
    }

    @Test
    void shouldFailOnCursorColumnNotInSchema() {
        PipelineConfig config = new PipelineConfig();
        PipelineConfig.Pipeline p = new PipelineConfig.Pipeline(); p.setName("test");
        config.setPipeline(p);

        DataSourceConfig.JdbcDataSource ds = new DataSourceConfig.JdbcDataSource();
        DataSourceConfig.CursorConfig cursor = new DataSourceConfig.CursorConfig();
        cursor.setColumn("missing_col"); cursor.setPageSize(100);
        ds.setCursor(cursor);
        ds.setConnection(new ConnectionConfig());
        config.setDatasource(ds);

        SchemaConfig schema = new SchemaConfig();
        schema.setFields(List.of(field("id", "LONG"), field("name", "STRING")));
        config.setInputSchema(schema);

        List<String> issues = config.validate();
        assertTrue(issues.stream().anyMatch(i -> i.contains("missing_col")));
    }

    @Test
    void shouldPassValidConfig() {
        PipelineConfig config = new PipelineConfig();
        PipelineConfig.Pipeline p = new PipelineConfig.Pipeline(); p.setName("test");
        config.setPipeline(p);

        DataSourceConfig.CsvDataSource ds = new DataSourceConfig.CsvDataSource();
        ds.setFilePath("/tmp/test.csv");
        config.setDatasource(ds);

        SchemaConfig schema = new SchemaConfig();
        schema.setFields(List.of(field("id", "LONG")));
        config.setInputSchema(schema);

        List<String> issues = config.validate();
        assertTrue(issues.isEmpty(), "Expected no issues but got: " + issues);
    }

    @Test
    void shouldFailOnTransformFieldNotInSchema() {
        PipelineConfig config = new PipelineConfig();
        PipelineConfig.Pipeline p = new PipelineConfig.Pipeline(); p.setName("test");
        config.setPipeline(p);

        DataSourceConfig.CsvDataSource ds = new DataSourceConfig.CsvDataSource();
        ds.setFilePath("/tmp/test.csv");
        config.setDatasource(ds);

        SchemaConfig schema = new SchemaConfig();
        schema.setFields(List.of(field("id", "LONG")));
        config.setInputSchema(schema);

        TransformDef.RenameDef rename = new TransformDef.RenameDef();
        TransformDef.MappingDef m = new TransformDef.MappingDef(); m.setFrom("nonexistent"); m.setTo("x");
        rename.setMappings(List.of(m));
        config.setTransforms(List.of(rename));

        List<String> issues = config.validate();
        assertTrue(issues.stream().anyMatch(i -> i.contains("nonexistent")));
    }

    private SchemaConfig.FieldDef field(String name, String type) {
        SchemaConfig.FieldDef f = new SchemaConfig.FieldDef();
        f.setName(name); f.setType(type);
        return f;
    }
}
