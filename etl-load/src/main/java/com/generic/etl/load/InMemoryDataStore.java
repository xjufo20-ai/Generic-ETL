package com.generic.etl.load;

import com.generic.etl.common.model.Row;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory store for PULL-mode data access.
 * In production, this would be backed by Redis or similar.
 */
public class InMemoryDataStore {
    private static final Map<String, List<Row>> store = new ConcurrentHashMap<>();

    public static void put(String pipeline, List<Row> rows) {
        store.put(pipeline, rows);
    }

    public static List<Row> get(String pipeline) {
        return store.getOrDefault(pipeline, List.of());
    }

    public static void clear(String pipeline) {
        store.remove(pipeline);
    }
}
