package com.generic.etl.core.transform;

import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Captures rows that fail transform without aborting the pipeline.
 * Failed rows are logged and optionally stored for later inspection/reprocessing.
 */
@Slf4j
public class DeadLetterQueue {
    private final Map<String, List<FailedRow>> failures = new ConcurrentHashMap<>();
    private final int maxFailures;

    public DeadLetterQueue() { this(1000); }
    public DeadLetterQueue(int maxFailures) { this.maxFailures = maxFailures; }

    /**
     * Process rows through a transform, capturing failures.
     * @return stream of successfully processed rows
     */
    public Stream<Row> filterWithDLQ(Stream<Row> rows, String pipeline, TransformProcessor processor, com.generic.etl.common.model.TransformDef def) {
        return rows.map(row -> {
            try {
                Row result = processor.process(row, def);
                if (result == null) return null; // filtered by expression, not an error
                return result;
            } catch (Exception e) {
                List<FailedRow> list = failures.computeIfAbsent(pipeline, k -> new ArrayList<>());
                if (list.size() < maxFailures) list.add(new FailedRow(row, e.getMessage()));
                log.warn("DLQ: row failed in '{}': {}", pipeline, e.getMessage());
                return null;
            }
        }).filter(r -> r != null);
    }

    public List<FailedRow> getFailures(String pipeline) { return failures.getOrDefault(pipeline, List.of()); }
    public void clear(String pipeline) { failures.remove(pipeline); }

    public record FailedRow(Row row, String error) {}
}
