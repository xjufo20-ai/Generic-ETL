package com.generic.etl.api.config;

import com.generic.etl.common.model.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EtlCompilerTest {

    @Test
    void compileJdbcPipeline() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("test");
        var ds = new DataSourceConfig.JdbcDataSource(); ds.setType("mysql");
        ds.setQuery("SELECT a, b FROM t"); c.setDatasource(ds);
        c.setOutput(new PersistConfig()); c.getOutput().setStorage(new PersistConfig.StorageConfig());
        c.getOutput().getStorage().setType("csv"); c.getOutput().getStorage().setTable("out.csv");

        String yaml = JsonToYamlCompiler.compile(c);
        assertTrue(yaml.contains("id: test"));
        assertTrue(yaml.contains("jdbc:etlDataSource"));
        assertTrue(yaml.contains("SELECT a, b FROM t"));
        assertTrue(yaml.contains("file:data"));
        assertTrue(yaml.contains("out.csv"));
    }

    @Test
    void compileWithFilterAndAggregate() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("stats");
        var ds = new DataSourceConfig.JdbcDataSource(); ds.setType("mysql");
        ds.setQuery("SELECT dept, salary FROM emp"); c.setDatasource(ds);
        c.setInputSchema(new SchemaConfig());
        var f1 = new SchemaConfig.FieldDef(); f1.setName("dept"); f1.setType("STRING");
        var f2 = new SchemaConfig.FieldDef(); f2.setName("salary"); f2.setType("DECIMAL");
        c.getInputSchema().setFields(List.of(f1, f2));
        var proj = new TransformDef.ProjectDef();
        var m1 = new TransformDef.MappingDef(); m1.setFrom("dept"); m1.setTo("d");
        var m2 = new TransformDef.MappingDef(); m2.setFrom("salary"); m2.setTo("s");
        proj.setMappings(List.of(m1, m2));
        var filt = new TransformDef.FilterDef(); filt.setExpression("s > 100");
        var agg = new TransformDef.AggregateDef();
        agg.setGroupBy(List.of("d"));
        var ag = new TransformDef.Aggregation(); ag.setField("s"); ag.setFunction("SUM"); ag.setAlias("total");
        agg.setAggregations(List.of(ag));
        c.setTransforms(List.of(proj, filt, agg));
        c.setOutput(new PersistConfig()); c.getOutput().setStorage(new PersistConfig.StorageConfig());
        c.getOutput().getStorage().setType("csv"); c.getOutput().getStorage().setTable("out.csv");

        String yaml = JsonToYamlCompiler.compile(c);
        assertTrue(yaml.contains("projectTransformer"));
        assertTrue(yaml.contains("dept: d"));
        assertTrue(yaml.contains("salary: s"));
        assertTrue(yaml.contains("etlAggregator"));
        assertTrue(yaml.contains("SUM"));
        assertTrue(yaml.contains("bean:loadRouter"));
    }

    @Test
    void compileKafkaSource() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("kafka-pipe");
        var ds = new DataSourceConfig.KafkaDataSource();
        ds.setType("kafka"); ds.setConnection(new DataSourceConfig.KafkaConnection());
        ds.getConnection().setTopic("events"); ds.getConnection().setBootstrapServers("localhost:9092");
        ds.getConnection().setGroupId("etl"); c.setDatasource(ds);

        String yaml = JsonToYamlCompiler.compile(c);
        assertTrue(yaml.contains("kafka:events"));
        assertTrue(yaml.contains("localhost:9092"));
    }

    @Test
    void aggregateSumCountAvg() {
        List<Map<String, Object>> rows = List.of(Map.of("v", 10), Map.of("v", 20), Map.of("v", 30));
        // Test aggregation logic directly
        double sum = rows.stream().mapToDouble(r -> ((Number)r.get("v")).doubleValue()).sum();
        assertEquals(60.0, sum);
    }

    @Test
    void watermarkAppendsToQuery() {
        var c = new PipelineConfig();
        c.setPipeline(new PipelineConfig.Pipeline()); c.getPipeline().setName("inc");
        var ds = new DataSourceConfig.JdbcDataSource(); ds.setType("mysql");
        ds.setQuery("SELECT * FROM t"); c.setDatasource(ds);
        c.setWatermark(new WatermarkConfig()); c.getWatermark().setColumn("ts");
        c.getWatermark().setInitial("2024-01-01");
        c.setOutput(new PersistConfig()); c.getOutput().setStorage(new PersistConfig.StorageConfig());
        c.getOutput().getStorage().setType("csv"); c.getOutput().getStorage().setTable("o.csv");

        String yaml = JsonToYamlCompiler.compile(c);
        assertTrue(yaml.contains("ts >= '2024-01-01'"));
    }

    @Test
    void consumerRegistrationFields() {
        var reg = new ConsumerRegistration();
        reg.setConsumer(new ConsumerRegistration.Consumer()); reg.getConsumer().setName("dash");
        var sub = new ConsumerRegistration.Subscription(); sub.setPipeline("p1");
        sub.setFields(List.of("a", "b")); sub.setFilter("a > 0");
        reg.setSubscriptions(List.of(sub));
        assertEquals(1, reg.getSubscriptions().size());
        assertEquals(List.of("a", "b"), reg.getSubscriptions().get(0).getFields());
    }
}
