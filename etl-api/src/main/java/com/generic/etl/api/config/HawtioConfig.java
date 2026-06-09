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
import java.util.Map;

/**
 * Serves the Hawtio console at /hawtio/.
 * Hawtio 4.x bundles static resources under "hawtio-static/" in the jar,
 * so we need to prepend that prefix when resolving classpath resources.
 */
@Configuration
public class HawtioConfig {

    private static final Map<String, String> CONTENT_TYPES = Map.of(
        "html", "text/html; charset=UTF-8",
        "css", "text/css",
        "js", "application/javascript",
        "json", "application/json",
        "png", "image/png",
        "svg", "image/svg+xml",
        "woff2", "font/woff2",
        "woff", "font/woff",
        "ttf", "font/ttf",
        "eot", "application/vnd.ms-fontobject"
    );

    @Bean
    public ServletRegistrationBean<HawtioServlet> hawtioServletRegistration() {
        ServletRegistrationBean<HawtioServlet> reg =
            new ServletRegistrationBean<>(new HawtioServlet(), "/hawtio/*");
        reg.setLoadOnStartup(1);
        reg.setName("hawtioServlet");
        return reg;
    }

    public static class HawtioServlet extends HttpServlet {

        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String path = req.getRequestURI();
            String ctxPath = req.getContextPath();
            String servletPath = "/hawtio";
            String resource = path.substring((ctxPath + servletPath).length());
            if (resource.isEmpty() || resource.equals("/")) {
                resource = "/index.html";
            }

            // Try classpath locations — Hawtio 4.x uses "hawtio-static/" prefix
            InputStream in = findResource(resource);
            if (in == null) {
                resp.sendError(404, "Hawtio resource not found: " + resource);
                return;
            }

            String ext = resource.contains(".")
                ? resource.substring(resource.lastIndexOf('.') + 1).toLowerCase()
                : "";
            resp.setContentType(CONTENT_TYPES.getOrDefault(ext, "application/octet-stream"));

            try (InputStream is = in) {
                StreamUtils.copy(is, resp.getOutputStream());
            }
        }

        private InputStream findResource(String resource) {
            ClassLoader cl = getClass().getClassLoader();
            // Hawtio 4.x static resources are under "hawtio-static/" in the jar
            String[] prefixes = {
                "hawtio-static",
                "hawtio",
                "static/hawtio",
                "META-INF/resources/hawtio",
                "META-INF/resources/hawtio-static"
            };
            for (String prefix : prefixes) {
                InputStream in = cl.getResourceAsStream(prefix + resource);
                if (in != null) return in;
            }
            return null;
        }
    }
}
