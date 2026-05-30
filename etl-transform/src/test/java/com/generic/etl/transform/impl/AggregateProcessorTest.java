package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class AggregateProcessorTest {
    private final AggregateProcessor processor = new AggregateProcessor();

    @Test
    void shouldAggregateWithGroupBy() {
        List<Row> rows = List.of(row("A", new BigDecimal("100")), row("A", new BigDecimal("200")), row("B", new BigDecimal("50")));
        TransformDef.AggregateDef def = new TransformDef.AggregateDef();
        def.setGroupBy(List.of("dept"));
        TransformDef.Aggregation a = new TransformDef.Aggregation(); a.setField("salary"); a.setFunction("SUM"); a.setAlias("total_salary");
        def.setAggregations(List.of(a));
        List<Row> results = processor.processSet(rows, def);
        assertEquals(2, results.size());
        assertEquals(new BigDecimal("300"), results.get(0).get("total_salary"));
        assertEquals("A", results.get(0).get("dept"));
        assertEquals(new BigDecimal("50"), results.get(1).get("total_salary"));
    }

    @Test
    void shouldCountRows() {
        List<Row> rows = List.of(row("A", BigDecimal.ONE), row("A", BigDecimal.ONE));
        TransformDef.AggregateDef def = new TransformDef.AggregateDef();
        def.setGroupBy(List.of("dept"));
        TransformDef.Aggregation a = new TransformDef.Aggregation(); a.setField("id"); a.setFunction("COUNT"); a.setAlias("cnt");
        def.setAggregations(List.of(a));
        List<Row> results = processor.processSet(rows, def);
        assertEquals(1, results.size());
        assertEquals(2L, results.get(0).get("cnt"));
    }

    private Row row(String dept, BigDecimal salary) { Row r = new Row(); r.put("dept", dept); r.put("salary", salary); r.put("id", 1L); return r; }
}
