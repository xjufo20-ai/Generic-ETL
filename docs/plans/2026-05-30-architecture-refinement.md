# Architecture Refinement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove dead code, fix InMemoryDataStore to be a proper Spring bean, refactor TransformDef to polymorphic deserialization, fix OutputConfig naming, add pagination to consumer data API.

**Architecture:** Clean up dead weight (PipelineRunner, JoinConfig), fix static singleton (InMemoryDataStore → bean), make TransformDef type-safe like DataSourceConfig already does with @JsonTypeInfo.

**Tech Stack:** Java 22, Spring Boot 3.3.5, Gradle, Jackson, Lombok

---

### Task 1: Remove dead code (PipelineRunner, JoinConfig)

**Files:**
- Delete: `etl-core/src/main/java/com/generic/etl/core/runner/PipelineRunner.java`
- Modify: `etl-common/src/main/java/com/generic/etl/common/model/JoinConfig.java` → DELETE
- Modify: `etl-common/src/main/java/com/generic/etl/common/model/PipelineConfig.java` (remove `join` field)

**Step 1: Delete PipelineRunner**
```bash
rm etl-core/src/main/java/com/generic/etl/core/runner/PipelineRunner.java
```

**Step 2: Delete JoinConfig and remove PipelineConfig.join field**
Delete `etl-common/src/main/java/com/generic/etl/common/model/JoinConfig.java`.
Remove the field from PipelineConfig:
```java
// REMOVE this line from PipelineConfig.java:
private JoinConfig join;
// REMOVE this import:
import com.generic.etl.common.model.JoinConfig;
```

**Step 3: Verify build**
```bash
./gradlew build --no-daemon
```
Expected: BUILD SUCCESSFUL

**Step 4: Commit**
```bash
git add -A && git commit -m "chore: remove dead code (PipelineRunner, JoinConfig)"
```

---

### Task 2: Refactor InMemoryDataStore to Spring bean

**Files:**
- Modify: `etl-load/src/main/java/com/generic/etl/load/InMemoryDataStore.java`
- Modify: `etl-api/src/main/java/com/generic/etl/api/config/EtlConfig.java`
- Modify: `etl-load/src/main/java/com/generic/etl/load/LoadRouter.java`
- Modify: `etl-api/src/main/java/com/generic/etl/api/controller/ConsumerController.java`

**Step 1: Rewrite InMemoryDataStore as non-static Spring bean**

```java
package com.generic.etl.load;

import com.generic.etl.common.model.Row;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Spring-managed bean for PULL-mode consumer data with TTL eviction. */
public class InMemoryDataStore {
    private final Map<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final long ttlSeconds;

    public InMemoryDataStore() { this(3600); }
    public InMemoryDataStore(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }

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

    public void clear(String pipeline) { store.remove(pipeline); }
    public void evictExpired() { store.entrySet().removeIf(e -> e.getValue().expiresAt.isBefore(Instant.now())); }

    private record CacheEntry(List<Row> rows, Instant expiresAt) {}
}
```

Key changes:
- Remove `static` modifier from all methods
- Add TTL-based eviction (default 1 hour)
- Add `evictExpired()` method for scheduled cleanup

**Step 2: Register as Spring bean in EtlConfig**

Add to `EtlConfig.java`:
```java
@Bean
public InMemoryDataStore inMemoryDataStore() {
    return new InMemoryDataStore(3600); // 1 hour TTL
}
```

**Step 3: Update LoadRouter to use injected InMemoryDataStore**

Replace `InMemoryDataStore.put(pipelineName, rows)` with injected field usage:
```java
private final InMemoryDataStore inMemoryStore;
// ... inject via constructor ...
// In route(): inMemoryStore.put(pipelineName, rows);
```

**Step 4: Update ConsumerController to use injected InMemoryDataStore**

Replace `InMemoryDataStore.get(pipeline)` with injected field:
```java
private final InMemoryDataStore inMemoryStore;
// ... inject via constructor ...
// In pullData(): List<Row> rows = inMemoryStore.get(pipeline);
```

**Step 5: Add scheduled eviction to EtlApplication or a @Scheduled bean**

```java
@Scheduled(fixedRate = 60000)
public void evictExpired() { inMemoryDataStore.evictExpired(); }
```

**Step 6: Verify tests pass**
```bash
./gradlew build --no-daemon
```
Expected: BUILD SUCCESSFUL

**Step 7: Commit**
```bash
git add -A && git commit -m "refactor: InMemoryDataStore as Spring bean with TTL eviction"
```

---

### Task 3: TransformDef polymorphic deserialization

**Files:**
- Modify: `etl-common/src/main/java/com/generic/etl/common/model/TransformDef.java`
- Modify: `etl-transform/src/main/java/com/generic/etl/transform/impl/*.java`
- Modify: `etl-core/src/main/java/com/generic/etl/core/transform/TransformProcessor.java`
- Modify: config samples that use transforms

**Step 1: Rewrite TransformDef with @JsonTypeInfo**

Replace the current monolithic TransformDef with polymorphic types:
```java
package com.generic.etl.common.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TransformDef.FilterDef.class, name = "filter"),
    @JsonSubTypes.Type(value = TransformDef.RenameDef.class, name = "rename"),
    @JsonSubTypes.Type(value = TransformDef.TypeCastDef.class, name = "typeCast"),
    @JsonSubTypes.Type(value = TransformDef.AggregateDef.class, name = "aggregate"),
    @JsonSubTypes.Type(value = TransformDef.JoinDef.class, name = "join")
})
@Data
public abstract class TransformDef {
    protected String type;

    @Data
    public static class FilterDef extends TransformDef {
        private String expression;
    }

    @Data
    public static class RenameDef extends TransformDef {
        private List<MappingDef> mappings;
    }

    @Data
    public static class TypeCastDef extends TransformDef {
        private List<TypeCastMapping> mappings;
    }

    @Data
    public static class AggregateDef extends TransformDef {
        private List<String> groupBy;
        private List<Aggregation> aggregations;
    }

    @Data
    public static class JoinDef extends TransformDef {
        private String query;
        private String on;
        private String joinType; // INNER, LEFT
    }

    @Data
    public static class MappingDef {  // for rename
        private String from;
        private String to;
    }

    @Data
    public static class TypeCastMapping {  // for typeCast
        private String field;
        private String toType;
    }

    @Data
    public static class Aggregation {
        private String field;
        private String function; // SUM, AVG, COUNT, MIN, MAX
        private String alias;
    }
}
```

**Step 2: Update TransformProcessor interface**

Change from `Row process(Row row, TransformDef def)` to use concrete subclasses:
```java
public interface TransformProcessor {
    Row process(Row row, TransformDef def);
    default Stream<Row> processStream(Stream<Row> rows, TransformDef def) { ... }
    default boolean isSetProcessor() { return false; }
}
```
(No change needed — processors internally cast, but they used to cast from TransformDef fields. Now they'll use instanceof checks on the concrete subclass.)

**Step 3: Update each processor to use concrete types**

FilterProcessor: `if (def instanceof TransformDef.FilterDef f)`
RenameProcessor: `if (def instanceof TransformDef.RenameDef f)`
TypeCastProcessor: `if (def instanceof TransformDef.TypeCastDef f)`
AggregateProcessor: `if (def instanceof TransformDef.AggregateDef f)`
JoinProcessor: `if (def instanceof TransformDef.JoinDef f)`

**Step 4: Update config JSON samples**

Update all sample JSON files: transforms with only relevant fields per type. Already correct for the most part — filter only has `expression`, rename only has `mappings`, aggregate has `groupBy`+`aggregations`.

**Step 5: Verify build + tests**
```bash
./gradlew build --no-daemon
```
Expected: BUILD SUCCESSFUL, all tests pass

**Step 6: Commit**
```bash
git add -A && git commit -m "refactor: polymorphic TransformDef with @JsonTypeInfo"
```

---

### Task 4: Rename OutputConfig → PersistConfig and fix naming

**Files:**
- Rename: `etl-common/src/main/java/com/generic/etl/common/model/OutputConfig.java` → `PersistConfig.java`
- Modify: `etl-common/src/main/java/com/generic/etl/common/model/PipelineConfig.java`
- Modify: `etl-load/src/main/java/com/generic/etl/load/persist/PersistHandler.java`
- Modify: `etl-load/src/main/java/com/generic/etl/load/LoadRouter.java`
- Modify: `etl-api/src/main/java/com/generic/etl/api/config/PipelineExecutionService.java`

**Step 1: Rename file and class**

Rename `OutputConfig.java` to `PersistConfig.java`.
Change class name to `PersistConfig`.
Add `output` as the top-level wrapper if needed:
```java
package com.generic.etl.common.model;

@Data
public class PersistConfig {
    private boolean enabled;
    private int threshold = 0;
    private StorageConfig storage;

    @Data
    public static class StorageConfig {
        private String type; // postgresql, clickhouse
        private String table;
        private ConnectionConfig connection;
    }

    @Data
    public static class ConnectionConfig {
        private String url;
        private String username;
        private String password;
        private String driverClass;
    }
}
```

**Step 2: Update PipelineConfig**

Replace `private OutputConfig output;` with `private PersistConfig output;`

**Step 3: Update all usages**

Update imports in PersistHandler, LoadRouter, PipelineExecutionService from `OutputConfig` to `PersistConfig`.

**Step 4: Verify build**
```bash
./gradlew build --no-daemon
```
Expected: BUILD SUCCESSFUL

**Step 5: Commit**
```bash
git add -A && git commit -m "refactor: rename OutputConfig to PersistConfig"
```

---

### Task 5: Add pagination to consumer data API

**Files:**
- Modify: `etl-api/src/main/java/com/generic/etl/api/controller/ConsumerController.java`
- Modify: `etl-common/src/main/java/com/generic/etl/common/dto/DataResponse.java`

**Step 1: Update DataResponse to support pagination**

Add `page`, `pageSize`, `totalPages` fields to DataResponse:
```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataResponse {
    private String pipeline;
    private String consumer;
    private int totalRows;
    private int page;
    private int pageSize;
    private int totalPages;
    private List<Map<String, Object>> data;
    private String cursor;
    private boolean hasMore;
}
```

**Step 2: Update ConsumerController pullData with pagination**

```java
@GetMapping("/data/{pipeline}")
public ApiResponse<DataResponse> pullData(
    @PathVariable String pipeline,
    @RequestParam String consumer,
    @RequestParam(defaultValue = "0") int page,
    @RequestParam(defaultValue = "500") int pageSize) {

    List<Row> rows = inMemoryStore.get(pipeline);
    // ... projection logic ...
    
    int totalPages = (int) Math.ceil((double) projected.size() / pageSize);
    int from = page * pageSize;
    int to = Math.min(from + pageSize, projected.size());
    List<Map<String, Object>> pageData = projected.subList(from, to);
    
    DataResponse response = DataResponse.builder()
        .pipeline(pipeline).consumer(consumer)
        .totalRows(projected.size()).page(page).pageSize(pageSize)
        .totalPages(totalPages).data(pageData)
        .hasMore(page < totalPages - 1).build();
}
```

**Step 3: Verify build**
```bash
./gradlew build --no-daemon
```
Expected: BUILD SUCCESSFUL

**Step 4: Commit**
```bash
git add -A && git commit -m "feat: add pagination to consumer data API (page/pageSize)"
```
