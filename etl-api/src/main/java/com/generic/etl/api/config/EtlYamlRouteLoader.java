package com.generic.etl.api.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.spi.RoutesLoader;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scans config/routes/*.yaml at startup and loads them as Camel routes.
 * Resolves {{env:VAR}} placeholders before passing to Camel's native YAML DSL loader.
 *
 */
@Slf4j
@Component
public class EtlYamlRouteLoader {

    private static final String ROUTES_DIR = "config/routes";
    private static final Pattern ENV_PATTERN = Pattern.compile("\\{\\{env:([^}:]+)(?::([^}]*))?\\}\\}");

    private final CamelContext camelContext;

    public EtlYamlRouteLoader(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void loadRoutes() {
        Path dir = Path.of(ROUTES_DIR);
        if (!Files.exists(dir)) {
            log.info("Routes dir not found: {}", ROUTES_DIR);
            return;
        }

        try (var files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                 .forEach(this::loadRoute);
        } catch (IOException e) {
            log.error("Failed to scan routes dir", e);
        }

        log.info("Loaded {} YAML routes from {}", camelContext.getRoutes().size(), ROUTES_DIR);
    }

    private void loadRoute(Path file) {
        try {
            String raw = Files.readString(file);
            String resolved = resolveEnv(raw);
            RoutesLoader loader = camelContext.getCamelContextExtension()
                    .getContextPlugin(RoutesLoader.class);
            loader.loadRoutes(camelContext, new org.apache.camel.spi.Resource() {
                @Override
                public String getLocation() { return file.toString(); }
                @Override
                public java.io.InputStream getInputStream() {
                    return new java.io.ByteArrayInputStream(resolved.getBytes());
                }
            });
            log.info("Loaded YAML route: {}", file.getFileName());
        } catch (Exception e) {
            log.error("Failed to load route {}: {}", file.getFileName(), e.getMessage());
        }
    }

    /** Resolve {{env:VAR}} or {{env:VAR:default}} placeholders. */
    static String resolveEnv(String content) {
        Matcher m = ENV_PATTERN.matcher(content);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String var = m.group(1);
            String def = m.group(2);
            String val = System.getenv(var);
            m.appendReplacement(sb, Matcher.quoteReplacement(val != null ? val : def != null ? def : ""));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
