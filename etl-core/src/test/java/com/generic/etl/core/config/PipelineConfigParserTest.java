package com.generic.etl.core.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.generic.etl.common.model.PipelineConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigParserTest {

    private PipelineConfigParser parser;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        parser = new PipelineConfigParser(mapper);
    }

    @Test
    void shouldParseCompletePipelineJson() throws Exception {
        String json = """
        {
          "pipeline": { "name": "test-etl", "version": "1.0" },
          "datasource": {
            "type": "csv",
            "filePath": "/tmp/test.csv",
            "delimiter": ",",
            "hasHeader": true
          },
          "inputSchema": {
            "fields": [
              {"name": "id", "type": "LONG"},
              {"name": "name", "type": "STRING"}
            ]
          },
          "transforms": [
            {"type": "filter", "expression": "id > 0"},
            {"type": "rename", "mappings": [{"from": "name", "to": "full_name"}]},
            {"type": "aggregate", "groupBy": ["name"], "aggregations": [
              {"field": "id", "function": "COUNT", "alias": "cnt"}
            ]}
          ],
          "output": {
            "enabled": true,
            "threshold": 1000,
            "storage": {
              "type": "postgresql",
              "table": "output.test"
            }
          }
        }
        """;

        PipelineConfig config = parser.parseFromString(json);
        assertEquals("test-etl", config.getPipeline().getName());
        assertEquals("csv", config.getDatasource().getType());
        assertEquals(3, config.getTransforms().size());
        assertEquals("filter", config.getTransforms().get(0).getType());
        assertEquals("rename", config.getTransforms().get(1).getType());
        assertEquals("aggregate", config.getTransforms().get(2).getType());
        assertTrue(config.getOutput().isEnabled());
    }

    @Test
    void shouldResolveEnvPlaceholders() {
        // Set env var for test
        String original = "{{env:TEST_VAR}}";
        // No env set, should keep placeholder
        assertEquals(original, PipelineConfigParser.resolvePlaceholders(original));
    }
}
