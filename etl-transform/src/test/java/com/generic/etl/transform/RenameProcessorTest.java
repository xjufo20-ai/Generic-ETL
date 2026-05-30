package com.generic.etl.transform;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.transform.rename.RenameProcessor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RenameProcessorTest {

    private final RenameProcessor processor = new RenameProcessor();

    @Test
    void shouldRenameField() {
        Row row = new Row();
        row.put("old_name", "value");
        TransformDef def = new TransformDef();
        TransformDef.MappingDef mapping = new TransformDef.MappingDef();
        mapping.setFrom("old_name");
        mapping.setTo("new_name");
        def.setMappings(List.of(mapping));

        Row result = processor.process(row, def);
        assertNotNull(result);
        assertFalse(result.has("old_name"));
        assertEquals("value", result.get("new_name"));
    }
}
