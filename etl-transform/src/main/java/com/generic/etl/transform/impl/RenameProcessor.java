package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

public class RenameProcessor implements TransformProcessor {

    @Override
    public Row process(Row row, TransformDef def) {
        if (def.getMappings() == null) return row;
        for (TransformDef.MappingDef mapping : def.getMappings()) {
            if (row.has(mapping.getFrom())) {
                Object value = row.get(mapping.getFrom());
                row.remove(mapping.getFrom());
                row.put(mapping.getTo(), value);
            }
        }
        return row;
    }
}
