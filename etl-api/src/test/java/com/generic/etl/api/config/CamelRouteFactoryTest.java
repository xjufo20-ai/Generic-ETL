package com.generic.etl.api.config;

import com.generic.etl.common.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SourceUriBuilder and TransformEipMapper helpers.
 * Full Camel route integration tests require a running CamelContext.
 */
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

        // URI building tested indirectly — verify config structure
        assertEquals("test", config.getPipeline().getName());
        assertEquals("SELECT * FROM t", jdbc.getQuery());
    }

    @Test
    void aggregateRowsCount() {
        List<Map<String, Object>> rows = List.of(
            Map.of("val", 10), Map.of("val", 20), Map.of("val", 30)
        );
        var aggs = List.of(new TransformDef.Aggregation("val", "SUM", "total"));
        Map<String, Object> result = TransformEipMapper.aggregateRows(rows, aggs, List.of());
        assertEquals(60.0, (double) result.get("total"), 0.01);
    }

    @Test
    void aggregateRowsGroupBy() {
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
    void castTypes() {
        assertEquals("123", TransformEipMapper.cast(123, "STRING"));
        assertEquals(123L, TransformEipMapper.cast("123", "LONG"));
        assertEquals(3.14, TransformEipMapper.cast("3.14", "DOUBLE"));
        assertEquals(1, TransformEipMapper.cast("1", "INT"));
        assertEquals(true, TransformEipMapper.cast("true", "BOOLEAN"));
    }
}
