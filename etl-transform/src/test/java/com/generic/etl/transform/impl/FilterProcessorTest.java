package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FilterProcessorTest {
    private final FilterProcessor processor = new FilterProcessor();

    @Test
    void shouldPassMatchingRow() {
        Row row = new Row(); row.put("age", 25);
        TransformDef.FilterDef def = new TransformDef.FilterDef(); def.setExpression("age > 18");
        assertNotNull(processor.process(row, def));
    }

    @Test
    void shouldFilterNonMatchingRow() {
        Row row = new Row(); row.put("age", 15);
        TransformDef.FilterDef def = new TransformDef.FilterDef(); def.setExpression("age > 18");
        assertNull(processor.process(row, def));
    }
}
