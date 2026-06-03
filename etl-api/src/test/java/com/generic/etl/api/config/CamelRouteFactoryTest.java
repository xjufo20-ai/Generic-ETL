package com.generic.etl.api.config;

import com.generic.etl.common.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CamelRouteFactoryTest {

    @Test
    void aggregateRowsSum() {
        List<Map<String, Object>> rows = List.of(Map.of("v", 10), Map.of("v", 20), Map.of("v", 30));
        var aggs = List.of(new TransformDef.Aggregation("v", "SUM", "total"));
        var r = TransformEipMapper.aggregateRows(rows, aggs, List.of());
        assertEquals(60.0, (double) r.get("total"), 0.01);
    }

    @Test
    void aggregateRowsCountGroupBy() {
        List<Map<String, Object>> rows = List.of(Map.of("d", "A", "v", 10), Map.of("d", "A", "v", 20), Map.of("d", "B", "v", 5));
        var aggs = List.of(new TransformDef.Aggregation("v", "COUNT", "cnt"));
        var r = TransformEipMapper.aggregateRows(rows, aggs, List.of("d"));
        assertEquals("A", r.get("d"));
        assertEquals(3L, r.get("cnt"));
    }

    @Test
    void aggregateAvgMinMax() {
        List<Map<String, Object>> rows = List.of(Map.of("v", 10), Map.of("v", 20), Map.of("v", 30), Map.of("v", 40));
        var aggs = List.of(new TransformDef.Aggregation("v", "AVG", "a"), new TransformDef.Aggregation("v", "MIN", "mn"), new TransformDef.Aggregation("v", "MAX", "mx"));
        var r = TransformEipMapper.aggregateRows(rows, aggs, List.of());
        assertEquals(25.0, (double) r.get("a"), 0.01);
        assertEquals(10.0, (double) r.get("mn"), 0.01);
        assertEquals(40.0, (double) r.get("mx"), 0.01);
    }

    @Test
    void aggregateEmpty() {
        var r = TransformEipMapper.aggregateRows(List.of(), List.of(new TransformDef.Aggregation("v", "COUNT", "c")), List.of());
        assertEquals(0L, r.get("c"));
    }

    @Test
    void castAllTypes() {
        assertEquals("123", TransformEipMapper.cast(123, "STRING"));
        assertEquals(123L, TransformEipMapper.cast("123", "LONG"));
        assertEquals(3.14, TransformEipMapper.cast("3.14", "DOUBLE"));
        assertEquals(1, TransformEipMapper.cast("1", "INT"));
        assertEquals(true, TransformEipMapper.cast("true", "BOOLEAN"));
        assertNull(TransformEipMapper.cast(null, "STRING"));
        assertEquals("x", TransformEipMapper.cast("x", "UNKNOWN"));
    }

    @Test
    void pipelineValidatesProjectAndOutputSchema() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("t");
        var ds = new DataSourceConfig.JdbcDataSource(); ds.setType("mysql");
        ds.setQuery("SELECT a, b FROM x"); c.setDatasource(ds);
        c.setInputSchema(new SchemaConfig());
        c.getInputSchema().setFields(List.of(new SchemaConfig.FieldDef("a", "LONG"), new SchemaConfig.FieldDef("b", "STRING")));
        // project: a→x, b→y
        var proj = new TransformDef.ProjectDef();
        proj.setMappings(List.of(new TransformDef.MappingDef("a", "x"), new TransformDef.MappingDef("b", "y")));
        // filter on canonical field
        var filt = new TransformDef.FilterDef(); filt.setExpression("x > 0");
        c.setTransforms(List.of(proj, filt));
        // outputSchema matches canonical
        c.setOutputSchema(new SchemaConfig());
        c.getOutputSchema().setFields(List.of(new SchemaConfig.FieldDef("x", "LONG"), new SchemaConfig.FieldDef("y", "STRING")));
        assertTrue(c.validate().isEmpty());
    }

    @Test
    void pipelineRejectsOutputSchemaNotInCanonical() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("t");
        var ds = new DataSourceConfig.JdbcDataSource(); ds.setType("mysql");
        ds.setQuery("SELECT a FROM x"); c.setDatasource(ds);
        c.setInputSchema(new SchemaConfig());
        c.getInputSchema().setFields(List.of(new SchemaConfig.FieldDef("a", "LONG")));
        c.setOutputSchema(new SchemaConfig());
        c.getOutputSchema().setFields(List.of(new SchemaConfig.FieldDef("z", "LONG"))); // not produced
        assertFalse(c.validate().isEmpty());
    }
}
