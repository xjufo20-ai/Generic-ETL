package com.generic.etl.transform.impl;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Splits a row into multiple rows by splitting a field value on a delimiter.
 * JSON: {"type": "split", "field": "tags", "delimiter": ","}
 */
public class SplitProcessor implements TransformProcessor {

    @Override
    public Row process(Row row, TransformDef def) {
        throw new UnsupportedOperationException("Split is a set-level transform");
    }

    @Override
    public boolean isSetProcessor() { return true; }

    @Override
    public List<Row> processSet(List<Row> rows, TransformDef def) {
        if (!(def instanceof TransformDef.SplitDef s)) return rows;
        String field = s.getField();
        String delimiter = s.getDelimiter() != null ? s.getDelimiter() : ",";

        List<Row> result = new ArrayList<>();
        for (Row row : rows) {
            Object val = row.get(field);
            if (val == null) { result.add(row); continue; }
            String[] parts = val.toString().split(delimiter);
            for (String part : parts) {
                Row cloned = row.copy();
                cloned.put(field, part.trim());
                result.add(cloned);
            }
        }
        return result;
    }
}
