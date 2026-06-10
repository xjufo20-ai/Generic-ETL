package com.generic.etl.core.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks data lineage: which pipeline → which output → consumed by whom, when.
 */
@Slf4j
public class LineageStore {
    private final Path file;
    private final ObjectMapper mapper;
    private final List<LineageEntry> entries = new ArrayList<>();

    public LineageStore(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("lineage.json");
        this.mapper = mapper;
        try { Files.createDirectories(dataDir); } catch (IOException e) { throw new RuntimeException(e); }
        load();
    }

    public void record(String pipeline, String outputTable, String consumer, long rows, String status) {
        LineageEntry e = new LineageEntry(pipeline, outputTable, consumer, rows, status, Instant.now().toString());
        synchronized (entries) { entries.add(e); }
        try { mapper.writeValue(file.toFile(), entries); } catch (Exception ex) { log.error("", ex); }
    }

    public List<LineageEntry> getAll() { return new ArrayList<>(entries); }
    public List<LineageEntry> getByPipeline(String pipeline) {
        return entries.stream().filter(e -> e.pipeline.equals(pipeline)).toList();
    }

    @SuppressWarnings("unchecked")
    private void load() {
        try { if (Files.exists(file)) entries.addAll(mapper.readValue(file.toFile(), List.class)); }
        catch (Exception e) { log.error("Failed to load lineage", e); }
    }

    public record LineageEntry(String pipeline, String outputTable, String consumer, long rows, String status, String timestamp) {}
}
