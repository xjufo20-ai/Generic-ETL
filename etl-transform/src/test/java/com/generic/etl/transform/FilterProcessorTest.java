package com.generic.etl.transform;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.transform.filter.FilterProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FilterProcessorTest {

    private final FilterProcessor processor = new FilterProcessor();

    @Test
    void shouldPassMatchingRow() {
        Row row = new Row();
        row.put("age", 25);
        TransformDef def = new TransformDef();
        def.setExpression("age > 18");

        Row result = processor.process(row, def);
        assertNotNull(result);
        assertEquals(25, result.get("age"));
    }

    @Test
    void shouldFilterNonMatchingRow() {
        Row row = new Row();
        row.put("age", 15);
        TransformDef def = new TransformDef();
        def.setExpression("age > 18");

        Row result = processor.process(row, def);
        assertNull(result);
    }
}
