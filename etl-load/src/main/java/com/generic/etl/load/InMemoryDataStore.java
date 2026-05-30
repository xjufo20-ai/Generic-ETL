package com.generic.etl.load;

import com.generic.etl.common.model.Row;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring-managed bean for PULL-mode consumer data with TTL-based eviction.
 * Each pipeline's data expires after the configured TTL (default 1 hour).
 */
public class InMemoryDataStore {
    private final Map<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final long ttlSeconds;

    public InMemoryDataStore() {
        this(3600);
    }

    public InMemoryDataStore(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public void put(String pipeline, List<Row> rows) {
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

    public void clear(String pipeline) {
        store.remove(pipeline);
    }

    /** Remove all expired entries. Called periodically by scheduler. */
    public void evictExpired() {
        store.entrySet().removeIf(e -> e.getValue().expiresAt.isBefore(Instant.now()));
    }

    private record CacheEntry(List<Row> rows, Instant expiresAt) {}
}
