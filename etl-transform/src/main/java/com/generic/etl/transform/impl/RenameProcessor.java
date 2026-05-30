package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

public class RenameProcessor implements TransformProcessor {
    @Override
    public Row process(Row row, TransformDef def) {
        if (def instanceof TransformDef.RenameDef r && r.getMappings() != null) {
            for (TransformDef.MappingDef m : r.getMappings()) {
                if (row.has(m.getFrom())) {
                    Object value = row.get(m.getFrom());
                    row.remove(m.getFrom());
                    row.put(m.getTo(), value);
                }
            }
        }
        return row;
    }
}
