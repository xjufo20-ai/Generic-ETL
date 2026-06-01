package com.generic.etl.extract.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks last-extracted watermark value per pipeline.
 * Persisted to disk so incremental extraction survives restarts.
 */
@Slf4j
public class WatermarkStore {
    private final Path file;
    private final ObjectMapper mapper;
    private final Map<String, String> watermarks = new ConcurrentHashMap<>();

    public WatermarkStore(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("watermarks.json");
        this.mapper = mapper;
        try { Files.createDirectories(dataDir); } catch (IOException e) { throw new RuntimeException(e); }
        load();
    }

    public String get(String pipeline) { return watermarks.get(pipeline); }

    public void set(String pipeline, String value) {
        watermarks.put(pipeline, value);
        try { mapper.writeValue(file.toFile(), watermarks); } catch (Exception e) { log.error("", e); }
    }

    @SuppressWarnings("unchecked")
    private void load() {
        try {
            if (Files.exists(file)) {
                watermarks.putAll(mapper.readValue(file.toFile(), Map.class));
                log.info("Loaded {} watermarks", watermarks.size());
            }
        } catch (Exception e) { log.error("Failed to load watermarks", e); }
    }
}
