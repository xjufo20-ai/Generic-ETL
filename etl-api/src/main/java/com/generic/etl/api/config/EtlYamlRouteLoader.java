package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.common.model.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.regex.*;

/** Loads routes from config/routes/*.yaml + config/samples/*.json at startup. */
@Slf4j
@Component
public class EtlYamlRouteLoader {

    private static final String ROUTES_DIR = "config/routes";
    private static final String SAMPLES_DIR = "config/samples";
    private static final Pattern ENV = Pattern.compile("\\{\\{env:([^}:]+)(?::([^}]*))?\\}\\}");

    private final CamelContext camelContext;
    private final ObjectMapper mapper;

    public EtlYamlRouteLoader(CamelContext camelContext, ObjectMapper mapper) {
        this.camelContext = camelContext;
        this.mapper = mapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void loadRoutes() {
        loadYamlDir();
        loadJsonDir();
        log.info("Routes loaded: {}", camelContext.getRoutes().size());
    }

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
                         String yaml = JsonToYamlCompiler.compile(config);
                         loadYaml(yaml, f.toString());
                         log.info("Compiled JSON → Camel: {}", f.getFileName());
                     } catch (Exception e) {
                         log.error("Failed: {}", f.getFileName(), e);
                     }
                 });
        } catch (IOException e) { log.error("Scan failed: {}", SAMPLES_DIR, e); }
    }

    /** Load a YAML string as Camel routes. */
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

    private static String readFile(Path p) { try { return Files.readString(p); } catch (IOException e) { throw new RuntimeException(e); } }

    private static org.apache.camel.spi.Resource resource(String content, String loc) {
        return new org.apache.camel.spi.Resource() {
            @Override public String getLocation() { return loc; }
            @Override public InputStream getInputStream() { return new ByteArrayInputStream(content.getBytes()); }
            @Override public boolean exists() { return true; }
            @Override public String getScheme() { return "yaml"; }
        };
    }
}
