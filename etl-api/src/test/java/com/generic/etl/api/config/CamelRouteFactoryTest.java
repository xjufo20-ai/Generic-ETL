package com.generic.etl.api.config;

import com.generic.etl.common.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CamelRouteFactoryTest {

    @Test
    void sourceUriJdbc() {
        var config = new PipelineConfig();
        config.setPipeline(new PipelineConfig.Pipeline());
        config.getPipeline().setName("test");
        var jdbc = new DataSourceConfig.JdbcDataSource();
        jdbc.setType("mysql");
        jdbc.setQuery("SELECT * FROM t");
        config.setDatasource(jdbc);
        assertEquals("test", config.getPipeline().getName());
        assertEquals("SELECT * FROM t", jdbc.getQuery());
    }

    @Test
    void aggregateRowsSum() {
        List<Map<String, Object>> rows = List.of(
            Map.of("val", 10), Map.of("val", 20), Map.of("val", 30)
        );
        var aggs = List.of(new TransformDef.Aggregation("val", "SUM", "total"));
        Map<String, Object> result = TransformEipMapper.aggregateRows(rows, aggs, List.of());
        assertEquals(60.0, (double) result.get("total"), 0.01);
    }

    @Test
    void aggregateRowsCountWithGroupBy() {
        List<Map<String, Object>> rows = List.of(
            Map.of("dept", "A", "val", 10),
            Map.of("dept", "A", "val", 20),
            Map.of("dept", "B", "val", 5)
        );
        var aggs = List.of(new TransformDef.Aggregation("val", "COUNT", "cnt"));
        Map<String, Object> result = TransformEipMapper.aggregateRows(rows, aggs, List.of("dept"));
        assertEquals("A", result.get("dept"));
        assertEquals(3L, result.get("cnt"));
    }

    @Test
    void aggregateRowsAvgMinMax() {
        List<Map<String, Object>> rows = List.of(
            Map.of("v", 10), Map.of("v", 20), Map.of("v", 30), Map.of("v", 40)
        );
        var aggs = List.of(
            new TransformDef.Aggregation("v", "AVG", "avg"),
            new TransformDef.Aggregation("v", "MIN", "min"),
            new TransformDef.Aggregation("v", "MAX", "max")
        );
        Map<String, Object> result = TransformEipMapper.aggregateRows(rows, aggs, List.of());
        assertEquals(25.0, (double) result.get("avg"), 0.01);
        assertEquals(10.0, (double) result.get("min"), 0.01);
        assertEquals(40.0, (double) result.get("max"), 0.01);
    }

    @Test
    void castTypes() {
        assertEquals("123", TransformEipMapper.cast(123, "STRING"));
        assertEquals(123L, TransformEipMapper.cast("123", "LONG"));
        assertEquals(3.14, TransformEipMapper.cast("3.14", "DOUBLE"));
        assertEquals(1, TransformEipMapper.cast("1", "INT"));
        assertEquals(true, TransformEipMapper.cast("true", "BOOLEAN"));
    }

    @Test
    void castNullReturnsNull() {
        assertNull(TransformEipMapper.cast(null, "STRING"));
        assertNull(TransformEipMapper.cast(null, "LONG"));
    }

    @Test
    void castUnknownTypeReturnsSame() {
        assertEquals("hello", TransformEipMapper.cast("hello", "UNKNOWN"));
    }

    @Test
    void aggregateEmptyRows() {
        var aggs = List.of(new TransformDef.Aggregation("v", "COUNT", "cnt"));
        Map<String, Object> result = TransformEipMapper.aggregateRows(List.of(), aggs, List.of());
        assertEquals(0L, result.get("cnt"));
    }

    @Test
    void pipelineConfigValidation() {
        var config = new PipelineConfig();
        config.setPipeline(new PipelineConfig.Pipeline());
        config.getPipeline().setName("test");
        config.setDatasource(new DataSourceConfig.JdbcDataSource());
        ((DataSourceConfig.JdbcDataSource) config.getDatasource()).setType("mysql");
        ((DataSourceConfig.JdbcDataSource) config.getDatasource()).setQuery("SELECT 1");
        config.setInputSchema(new SchemaConfig());
        config.getInputSchema().setFields(List.of(
            new SchemaConfig.FieldDef("id", FieldType.LONG)
        ));
        assertTrue(config.validate().isEmpty(), "Valid config should have no issues");
    }

    @Test
    void pipelineConfigValidationMissingName() {
        var config = new PipelineConfig();
        config.setDatasource(new DataSourceConfig.JdbcDataSource());
        ((DataSourceConfig.JdbcDataSource) config.getDatasource()).setType("mysql");
        config.setInputSchema(new SchemaConfig());
        config.getInputSchema().setFields(List.of(new SchemaConfig.FieldDef("id", FieldType.LONG)));
        var issues = config.validate();
        assertFalse(issues.isEmpty());
        assertTrue(issues.stream().anyMatch(i -> i.contains("pipeline.name")));
    }
}
