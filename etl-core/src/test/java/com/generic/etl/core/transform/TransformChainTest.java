package com.generic.etl.core.transform;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.transform.impl.AggregateProcessor;
import com.generic.etl.transform.impl.FilterProcessor;
import com.generic.etl.transform.impl.RenameProcessor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TransformChainTest {

    @Test
    void shouldApplyFilterThenRenameThenAggregate() {
        Map<String, TransformProcessor> processors = new LinkedHashMap<>();
        processors.put("filter", new FilterProcessor());
        processors.put("rename", new RenameProcessor());
        processors.put("aggregate", new AggregateProcessor());
        TransformChain chain = new TransformChain(processors);

        PipelineConfig config = new PipelineConfig();

        TransformDef.FilterDef filter = new TransformDef.FilterDef();
        filter.setExpression("salary > 0");

        TransformDef.RenameDef rename = new TransformDef.RenameDef();
        TransformDef.MappingDef m = new TransformDef.MappingDef();
        m.setFrom("name"); m.setTo("employee_name");
        rename.setMappings(List.of(m));

        TransformDef.AggregateDef agg = new TransformDef.AggregateDef();
        agg.setGroupBy(List.of("dept"));
        TransformDef.Aggregation a = new TransformDef.Aggregation();
        a.setField("salary"); a.setFunction("SUM"); a.setAlias("total_salary");
        agg.setAggregations(List.of(a));

        config.setTransforms(List.of(filter, rename, agg));

        Row r1 = new Row(); r1.put("id", 1L); r1.put("name", "Alice"); r1.put("dept", "Eng"); r1.put("salary", new BigDecimal("100"));
        Row r2 = new Row(); r2.put("id", 2L); r2.put("name", "Bob"); r2.put("dept", "Eng"); r2.put("salary", new BigDecimal("200"));
        Row r3 = new Row(); r3.put("id", 3L); r3.put("name", "Charlie"); r3.put("dept", "Sales"); r3.put("salary", new BigDecimal("0"));

        List<Row> results = chain.apply(Stream.of(r1, r2, r3), config).toList();
        assertEquals(1, results.size());
        assertEquals("Eng", results.get(0).get("dept"));
        assertEquals(new BigDecimal("300"), results.get(0).get("total_salary"));
    }
}
