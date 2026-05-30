package com.generic.etl.transform.aggregateprocessortest;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.transform.impl.AggregateProcessor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AggregateProcessorTest {

    private final AggregateProcessor processor = new AggregateProcessor();

    @Test
    void shouldAggregateWithGroupBy() {
        List<Row> rows = List.of(
                row("A", new BigDecimal("100")),
                row("A", new BigDecimal("200")),
                row("B", new BigDecimal("50"))
        );

        TransformDef def = new TransformDef();
        def.setGroupBy(List.of("dept"));
        TransformDef.AggregationDef agg = new TransformDef.AggregationDef();
        agg.setField("salary");
        agg.setFunction("SUM");
        agg.setAlias("total_salary");
        def.setAggregations(List.of(agg));

        List<Row> results = processor.processSet(rows, def);
        assertEquals(2, results.size());

        // Group A: 100+200=300
        Row rowA = results.get(0);
        assertEquals("A", rowA.get("dept"));
        assertTrue(((BigDecimal) rowA.get("total_salary")).compareTo(new BigDecimal("300")) == 0);

        // Group B: 50
        Row rowB = results.get(1);
        assertEquals("B", rowB.get("dept"));
        assertTrue(((BigDecimal) rowB.get("total_salary")).compareTo(new BigDecimal("50")) == 0);
    }

    @Test
    void shouldCountRows() {
        List<Row> rows = List.of(row("A", new BigDecimal("100")), row("A", new BigDecimal("200")));

        TransformDef def = new TransformDef();
        def.setGroupBy(List.of("dept"));
        TransformDef.AggregationDef agg = new TransformDef.AggregationDef();
        agg.setField("id");
        agg.setFunction("COUNT");
        agg.setAlias("cnt");
        def.setAggregations(List.of(agg));

        List<Row> results = processor.processSet(rows, def);
        assertEquals(1, results.size());
        assertEquals(2L, results.get(0).get("cnt"));
    }

    private Row row(String dept, BigDecimal salary) {
        Row r = new Row();
        r.put("dept", dept);
        r.put("salary", salary);
        r.put("id", 1L);
        return r;
    }
}
