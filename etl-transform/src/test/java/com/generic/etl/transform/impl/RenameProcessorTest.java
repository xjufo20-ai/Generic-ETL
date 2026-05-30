package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RenameProcessorTest {
    private final RenameProcessor processor = new RenameProcessor();

    @Test
    void shouldRenameField() {
        Row row = new Row(); row.put("old_name", "value");
        TransformDef.RenameDef def = new TransformDef.RenameDef();
        TransformDef.MappingDef m = new TransformDef.MappingDef(); m.setFrom("old_name"); m.setTo("new_name");
        def.setMappings(List.of(m));
        Row result = processor.process(row, def);
        assertFalse(result.has("old_name"));
        assertEquals("value", result.get("new_name"));
    }
}
