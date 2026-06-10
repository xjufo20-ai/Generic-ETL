package com.generic.etl.engine;

import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ResultCache {
    private final Map<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final long ttlSeconds;
    private final int maxRows;

    public ResultCache() { this(3600, 100_000); }
    public ResultCache(long ttlSeconds, int maxRows) {
        this.ttlSeconds = ttlSeconds;
        this.maxRows = maxRows;
    }

    public void put(String pipeline, List<Row> rows) {
        if (rows.size() > maxRows) {
            store.remove(pipeline);
            log.warn("Pipeline '{}': {} rows exceed maxRows ({}), data not cached — query from persisted table",
                    pipeline, rows.size(), maxRows);
            return;
        }
        store.put(pipeline, new CacheEntry(rows, Instant.now().plusSeconds(ttlSeconds)));
    }

    public List<Row> get(String pipeline) {
        CacheEntry entry = store.get(pipeline);
        if (entry == null || entry.expiresAt.isBefore(Instant.now())) {
            store.remove(pipeline);
            return List.of();
        }
        return entry.rows;
    }

    public void clear(String pipeline) { store.remove(pipeline); }
    public void evictExpired() {
        store.entrySet().removeIf(e -> {
            if (e.getValue().expiresAt.isBefore(Instant.now())) {
                log.debug("Evicting expired cache for pipeline '{}'", e.getKey());
                return true;
            }
            return false;
        });
    }

    private record CacheEntry(List<Row> rows, Instant expiresAt) {}
}
