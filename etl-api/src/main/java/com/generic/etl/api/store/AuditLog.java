package com.generic.etl.api.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Append-only audit log for pipeline config changes and executions.
 * Each entry is a single JSON line in data/audit.jsonl.
 */
@Slf4j
public class AuditLog {
    private final Path file;
    private final ObjectMapper mapper;

    public AuditLog(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("audit.jsonl");
        this.mapper = mapper;
        try { Files.createDirectories(dataDir); } catch (IOException e) { throw new RuntimeException(e); }
    }

    /** Record a pipeline config change (register/update/delete). */
    public void recordChange(String pipeline, String action, String who) {
        Entry e = new Entry(pipeline, action, who, null, null, Instant.now().toString());
        append(e);
    }

    /** Record a pipeline execution. */
    public void recordExecution(String pipeline, String status, long rows, String who) {
        Entry e = new Entry(pipeline, "EXECUTION", who, status, rows, Instant.now().toString());
        append(e);
    }

    public List<Entry> getHistory(String pipeline) {
        try {
            if (!Files.exists(file)) return List.of();
            return Files.readAllLines(file).stream()
                    .map(line -> { try { return mapper.readValue(line, Entry.class); } catch (Exception ex) { return null; } })
                    .filter(e -> e != null && (pipeline == null || pipeline.equals(e.pipeline)))
                    .collect(Collectors.toList());
        } catch (IOException ex) { return List.of(); }
    }

    public List<Entry> getHistory() { return getHistory(null); }

    private void append(Entry entry) {
        try {
            String line = mapper.writeValueAsString(entry) + "\n";
            Files.writeString(file, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) { log.error("Failed to write audit log", e); }
    }

    public record Entry(String pipeline, String action, String who, String status, Long rows, String timestamp) {}
}
