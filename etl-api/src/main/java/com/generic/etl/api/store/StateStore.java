package com.generic.etl.api.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.common.model.ConsumerRegistration;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class StateStore {
    private final Path dir;
    private final ObjectMapper mapper;
    private final AuditLog audit;
    private final Map<String, String> pipelines = new ConcurrentHashMap<>();
    private final List<ConsumerRegistration> consumers = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public StateStore(Path dataDir, ObjectMapper mapper, AuditLog audit) {
        this.dir = dataDir;
        this.mapper = mapper;
        this.audit = audit;
        try { Files.createDirectories(dir); } catch (IOException e) { throw new RuntimeException(e); }
        load();
    }

    public void putPipeline(String name, String json) {
        lock.writeLock().lock();
        try { pipelines.put(name, json); flushPipelines(); audit.recordChange(name, "REGISTER", "api"); }
        finally { lock.writeLock().unlock(); }
    }

    public void removePipeline(String name) {
        lock.writeLock().lock();
        try { pipelines.remove(name); flushPipelines(); audit.recordChange(name, "DELETE", "api"); }
        finally { lock.writeLock().unlock(); }
    }

    public String getPipeline(String name) { return pipelines.get(name); }
    public Map<String, String> getAllPipelines() { return new LinkedHashMap<>(pipelines); }

    public void addConsumer(ConsumerRegistration reg) {
        lock.writeLock().lock();
        try { consumers.add(reg); flushConsumers(); }
        finally { lock.writeLock().unlock(); }
    }

    public void removeConsumer(String name) {
        lock.writeLock().lock();
        try { consumers.removeIf(c -> c.getConsumer().getName().equals(name)); flushConsumers(); }
        finally { lock.writeLock().unlock(); }
    }

    public List<ConsumerRegistration> getAllConsumers() { return new ArrayList<>(consumers); }
    public AuditLog getAudit() { return audit; }

    @SuppressWarnings("unchecked")
    private void load() {
        try {
            Path pf = dir.resolve("pipelines.json");
            if (Files.exists(pf)) { pipelines.putAll(mapper.readValue(pf.toFile(), Map.class)); log.info("Loaded {} pipelines", pipelines.size()); }
            Path cf = dir.resolve("consumers.json");
            if (Files.exists(cf)) {
                for (ConsumerRegistration r : mapper.readValue(cf.toFile(), ConsumerRegistration[].class)) consumers.add(r);
                log.info("Loaded {} consumers", consumers.size());
            }
        } catch (Exception e) { log.error("Failed to load state", e); }
    }

    private void flushPipelines() { try { mapper.writeValue(dir.resolve("pipelines.json").toFile(), pipelines); } catch (Exception e) { log.error("", e); } }
    private void flushConsumers() { try { mapper.writeValue(dir.resolve("consumers.json").toFile(), consumers); } catch (Exception e) { log.error("", e); } }
}
