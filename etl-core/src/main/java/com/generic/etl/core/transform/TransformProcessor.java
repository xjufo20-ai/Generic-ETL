package com.generic.etl.core.transform;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import java.util.List;
import java.util.stream.Stream;

public interface TransformProcessor {
    /** Apply transform to a single row. Returns null if row is filtered out. */
    Row process(Row row, TransformDef def);

    /** Apply transform to a stream of rows. */
    default Stream<Row> processStream(Stream<Row> rows, TransformDef def) {
        return rows.map(r -> process(r, def)).filter(r -> r != null);
    }

    /** Whether this processor changes row cardinality (e.g., aggregation). */
    default boolean isSetProcessor() {
        return false;
    }

    /** For set processors: convert stream to result rows. */
    default List<Row> processSet(List<Row> rows, TransformDef def) {
        throw new UnsupportedOperationException("Not a set processor");
    }
}
