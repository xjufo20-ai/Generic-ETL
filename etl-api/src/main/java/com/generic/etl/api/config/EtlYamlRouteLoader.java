package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.compile.JsonToYamlCompiler;
import com.generic.etl.engine.ConsumerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/**
 * Loads Camel routes and consumers at startup:
 * 1. config/routes/*.yaml             — static YAML DSL
 * 2. config/samples/*.json            — static pipeline JSON → compiled to Camel YAML
 * 3. config/consumers/*.json          — static consumer registrations
 * 4. data/pipelines.json              — API-registered pipelines, rehydrated on restart
 * 5. data/consumers.json              — API-registered consumers, rehydrated on restart
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EtlYamlRouteLoader {

    private static final String ROUTES_DIR = "config/routes";
    private static final String SAMPLES_DIR = "config/samples";
    private static final String CONSUMERS_DIR = "config/consumers";
    private static final Pattern ENV = Pattern.compile("\\{\\{env:([^}:]+)(?::([^}]*))?\\}\\}");

    private final CamelContext camelContext;
    private final ObjectMapper mapper;
    private final StateStore stateStore;
    private final ConsumerRegistry consumerRegistry;

    @EventListener(ApplicationReadyEvent.class)
    public void loadRoutes() {
        int before = camelContext.getRoutes().size();

        loadYamlDir();                                    // 1. static YAML
        loadJsonDir();                                    // 2. static pipeline JSON
        loadConsumerDir();                                // 3. static consumer JSON
        restoreApiPipelines();                            // 4. rehydrate API pipelines
        restoreConsumers();                               // 5. rehydrate API consumers

        log.info("Routes loaded: {} → {} (static + restored)", before, camelContext.getRoutes().size());
    }

    // ── Static YAML files ─────────────────────────────────────────────

    private void loadYamlDir() {
        Path dir = Path.of(ROUTES_DIR);
        if (!Files.exists(dir)) return;
        try (var files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                 .forEach(f -> loadYaml(resolveEnv(readFile(f)), f.toString()));
        } catch (IOException e) { log.error("Scan failed: {}", ROUTES_DIR, e); }
    }

    private void loadJsonDir() {
        Path dir = Path.of(SAMPLES_DIR);
        if (!Files.exists(dir)) return;
        try (var files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(".json"))
                 .forEach(f -> {
                     try {
                         String json = resolveEnv(readFile(f));
                         PipelineConfig config = mapper.readValue(json, PipelineConfig.class);
                         loadYaml(JsonToYamlCompiler.compile(config), f.toString());
                         log.info("Compiled JSON → Camel: {}", f.getFileName());
                     } catch (Exception e) {
                         log.error("Failed: {}", f.getFileName(), e);
                     }
                 });
        } catch (IOException e) { log.error("Scan failed: {}", SAMPLES_DIR, e); }
    }

    // ── Static consumer JSON ─────────────────────────────────────────

    private void loadConsumerDir() {
        Path dir = Path.of(CONSUMERS_DIR);
        if (!Files.exists(dir)) return;
        try (var files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(".json"))
                 .forEach(f -> {
                     try {
                         String json = resolveEnv(readFile(f));
                         ConsumerRegistration reg = mapper.readValue(json, ConsumerRegistration.class);
                         consumerRegistry.register(reg);
                         log.info("Registered consumer: {}", f.getFileName());
                     } catch (Exception e) {
                         log.error("Failed: {}", f.getFileName(), e);
                     }
                 });
        } catch (IOException e) { log.error("Scan failed: {}", CONSUMERS_DIR, e); }
    }

    // ── API-registered pipelines (rehydrate on restart) ──────────────

    private void restoreApiPipelines() {
        Map<String, String> stored = stateStore.getAllPipelines();
        if (stored.isEmpty()) return;

        int restored = 0;
        Set<String> existing = existingRouteIds();

        for (var entry : stored.entrySet()) {
            String name = entry.getKey();
            String json = entry.getValue();

            if (existing.contains(name)) {
                log.info("Skipping '{}' (route already loaded from static config)", name);
                continue;
            }

            try {
                PipelineConfig config = mapper.readValue(
                        resolveEnv(json), PipelineConfig.class);
                String yaml = JsonToYamlCompiler.compile(config);
                loadYaml(yaml, "api:" + name + ".yaml");
                restored++;
                log.info("Restored API pipeline → Camel: {}", name);
            } catch (Exception e) {
                log.error("Failed to restore pipeline '{}'", name, e);
            }
        }
        log.info("Restored {} API pipelines", restored);
    }

    // ── Consumer rehydration ──────────────────────────────────────────

    private void restoreConsumers() {
        List<ConsumerRegistration> stored = stateStore.getAllConsumers();
        if (stored.isEmpty()) return;

        int restored = 0;
        for (ConsumerRegistration reg : stored) {
            consumerRegistry.register(reg);
            restored++;
        }
        log.info("Restored {} consumers", restored);
    }

    // ── Public helpers ────────────────────────────────────────────────

    /** Load a YAML string as Camel routes (public — used by PipelineController). */
    public void loadYaml(String yaml, String location) {
        try {
            var loader = camelContext.getCamelContextExtension()
                .getContextPlugin(org.apache.camel.spi.RoutesLoader.class);
            loader.loadRoutes(resource(yaml, location));
        } catch (Exception e) { log.error("Load failed: {}", location, e); }
    }

    public static String resolveEnv(String s) {
        Matcher m = ENV.matcher(s); StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String v = System.getenv(m.group(1));
            String d = m.group(2);
            m.appendReplacement(sb, Matcher.quoteReplacement(v != null ? v : d != null ? d : ""));
        }
        m.appendTail(sb); return sb.toString();
    }

    private Set<String> existingRouteIds() {
        return new HashSet<>(camelContext.getRoutes().stream()
                .map(r -> r.getRouteId()).toList());
    }

    private static String readFile(Path p) {
        try { return Files.readString(p); } catch (IOException e) { throw new RuntimeException(e); }
    }

    private static org.apache.camel.spi.Resource resource(String content, String loc) {
        // Camel resolves RoutesBuilderLoader by file extension, so ensure .yaml
        String yamlLoc = loc.replaceFirst("\\.json$", ".yaml");
        return new org.apache.camel.spi.Resource() {
            @Override public String getLocation() { return yamlLoc; }
            @Override public InputStream getInputStream() { return new ByteArrayInputStream(content.getBytes()); }
            @Override public boolean exists() { return true; }
            @Override public String getScheme() { return "mem"; }
        };
    }
}
