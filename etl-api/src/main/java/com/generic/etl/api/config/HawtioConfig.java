package com.generic.etl.api.config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Enumeration;
import java.util.Map;

/**
 * Serves the Hawtio console at /hawtio/ without authentication.
 *
 * Hawtio 4.x ManagementConfiguration is excluded (see application.yml).
 * We handle everything manually:
 *   - Static resources from hawtio-springboot jar ("hawtio-static/")
 *   - Auth mock endpoints (/hawtio/user, /hawtio/auth/config)
 *   - Jolokia proxy (/hawtio/jolokia/* → /actuator/jolokia/*)
 */
@Configuration
public class HawtioConfig {

    private static final Map<String, String> CONTENT_TYPES = Map.ofEntries(
        Map.entry("html", "text/html; charset=UTF-8"),
        Map.entry("css", "text/css"),
        Map.entry("js", "application/javascript"),
        Map.entry("json", "application/json"),
        Map.entry("png", "image/png"),
        Map.entry("svg", "image/svg+xml"),
        Map.entry("woff2", "font/woff2"),
        Map.entry("woff", "font/woff"),
        Map.entry("ttf", "font/ttf"),
        Map.entry("eot", "application/vnd.ms-fontobject"),
        Map.entry("ico", "image/x-icon"),
        Map.entry("map", "application/json"),
        Map.entry("txt", "text/plain")
    );

    /** Serves Hawtio static resources and auth mock endpoints. */
    @Bean
    public ServletRegistrationBean<HawtioServlet> hawtioServletRegistration() {
        ServletRegistrationBean<HawtioServlet> reg =
            new ServletRegistrationBean<>(new HawtioServlet(), "/hawtio/*");
        reg.setLoadOnStartup(1);
        reg.setName("hawtioServlet");
        return reg;
    }

    /** Proxies Jolokia requests to Spring Boot Actuator. */
    @Bean
    public ServletRegistrationBean<JolokiaProxyServlet> jolokiaProxyServletRegistration() {
        ServletRegistrationBean<JolokiaProxyServlet> reg =
            new ServletRegistrationBean<>(new JolokiaProxyServlet(), "/hawtio/jolokia/*");
        reg.setLoadOnStartup(2);
        reg.setName("jolokiaProxyServlet");
        return reg;
    }

    // ── Static + auth servlet ─────────────────────────────────────────

    public static class HawtioServlet extends HttpServlet {
        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String path = req.getRequestURI();
            String ctxPath = req.getContextPath();
            String prefix = "/hawtio";
            String resource = path.substring((ctxPath + prefix).length());

            // Auth endpoints — tell Hawtio JS no auth is needed
            if ("/user".equals(resource)) {
                resp.setContentType("application/json");
                resp.getWriter().write("\"public\"");
                return;
            }
            if ("/auth/config".equals(resource)) {
                resp.setContentType("application/json");
                resp.getWriter().write("{\"authenticationEnabled\":false}");
                return;
            }

            // Static resources
            if (resource.isEmpty() || resource.equals("/")) {
                resource = "/index.html";
            }

            InputStream in = findResource(resource);
            if (in == null) {
                resp.sendError(404, "Hawtio resource not found: " + resource);
                return;
            }

            String ext = resource.contains(".")
                ? resource.substring(resource.lastIndexOf('.') + 1).toLowerCase()
                : "html";
            resp.setContentType(CONTENT_TYPES.getOrDefault(ext, "application/octet-stream"));
            try (InputStream is = in) {
                StreamUtils.copy(is, resp.getOutputStream());
            }
        }

        private InputStream findResource(String resource) {
            ClassLoader cl = getClass().getClassLoader();
            for (String prefix : new String[] {
                "hawtio-static", "hawtio",
                "static/hawtio", "META-INF/resources/hawtio",
                "META-INF/resources/hawtio-static"
            }) {
                InputStream in = cl.getResourceAsStream(prefix + resource);
                if (in != null) return in;
            }
            return null;
        }
    }

    // ── Jolokia proxy ─────────────────────────────────────────────────

    /**
     * Forwards /hawtio/jolokia/* → /actuator/jolokia/*
     * (Spring Boot Actuator auto-configures Jolokia when jolokia-core is present).
     */
    public static class JolokiaProxyServlet extends HttpServlet {
        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String path = req.getRequestURI();
            String ctxPath = req.getContextPath();
            String hawtioPrefix = "/hawtio";
            String suffix = path.substring((ctxPath + hawtioPrefix).length()); // /jolokia/...

            String target = "http://localhost:8080/actuator" + suffix;
            if (req.getQueryString() != null) {
                target += "?" + req.getQueryString();
            }

            try {
                HttpURLConnection conn = (HttpURLConnection) URI.create(target).toURL().openConnection();
                conn.setRequestMethod(req.getMethod());
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(15000);

                // Copy request headers
                Enumeration<String> names = req.getHeaderNames();
                while (names.hasMoreElements()) {
                    String name = names.nextElement();
                    if (!"Host".equalsIgnoreCase(name) && !"Content-Length".equalsIgnoreCase(name)) {
                        conn.setRequestProperty(name, req.getHeader(name));
                    }
                }

                // Copy request body (POST)
                if ("POST".equalsIgnoreCase(req.getMethod())) {
                    conn.setDoOutput(true);
                    try (OutputStream os = conn.getOutputStream()) {
                        StreamUtils.copy(req.getInputStream(), os);
                    }
                }

                // Copy response
                resp.setStatus(conn.getResponseCode());
                conn.getHeaderFields().forEach((key, values) -> {
                    if (key != null && !"Transfer-Encoding".equalsIgnoreCase(key)) {
                        values.forEach(v -> resp.addHeader(key, v));
                    }
                });

                InputStream body = conn.getResponseCode() >= 400
                    ? conn.getErrorStream() : conn.getInputStream();
                if (body != null) {
                    if (conn.getContentType() != null) resp.setContentType(conn.getContentType());
                    try (InputStream is = body) {
                        StreamUtils.copy(is, resp.getOutputStream());
                    }
                }
            } catch (Exception e) {
                resp.setContentType("application/json");
                resp.setStatus(502);
                resp.getWriter().write("{\"error\":\"Jolokia proxy error: " +
                    e.getMessage().replace("\"", "'") + "\"}");
            }
        }
    }
}
