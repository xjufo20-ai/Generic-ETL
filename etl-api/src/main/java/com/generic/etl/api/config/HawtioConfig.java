package com.generic.etl.api.config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jolokia.server.core.http.AgentServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Serves the Hawtio console at /hawtio/.
 *
 * Static resources and auth mocks are handled by HawtioServlet.
 * Jolokia AgentServlet (Jakarta-compatible, from Hawtio's jolokia-server-core:2.1.1)
 * is registered directly at /hawtio/jolokia/* so Hawtio auto-connects.
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

    /**
     * Registers Jolokia 2.1.1 AgentServlet (Jakarta-compatible) directly at /hawtio/jolokia/*.
     * Hawtio auto-connects to this local Jolokia agent without needing /connect/remote.
     */
    @Bean
    public ServletRegistrationBean<AgentServlet> jolokiaAgentRegistration() {
        ServletRegistrationBean<AgentServlet> reg =
            new ServletRegistrationBean<>(new AgentServlet(), "/hawtio/jolokia/*");
        reg.setLoadOnStartup(2);
        reg.setName("jolokiaAgent");
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
}
