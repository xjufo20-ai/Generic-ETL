package com.generic.etl.load;

import com.generic.etl.common.model.Row;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDataStore {
    private final Map<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final long ttlSeconds;
    private final int maxRows;

    public InMemoryDataStore() { this(3600, 100_000); }
    public InMemoryDataStore(long ttlSeconds, int maxRows) {
        this.ttlSeconds = ttlSeconds;
        this.maxRows = maxRows;
    }

    public void put(String pipeline, List<Row> rows) {
        if (rows.size() > maxRows) {
            store.remove(pipeline);
            return; // skip in-memory, consumer should query from persisted table
        }
        store.put(pipeline, new CacheEntry(rows, Instant.now().plusSeconds(ttlSeconds)));
    }

    public List<Row> get(String pipeline) {
        CacheEntry entry = store.get(pipeline);
        if (entry == null || entry.expiresAt.isBefore(Instant.now())) { store.remove(pipeline); return List.of(); }
        return entry.rows;
    }

    public void clear(String pipeline) { store.remove(pipeline); }
    public void evictExpired() { store.entrySet().removeIf(e -> e.getValue().expiresAt.isBefore(Instant.now())); }

    private record CacheEntry(List<Row> rows, Instant expiresAt) {}
}
