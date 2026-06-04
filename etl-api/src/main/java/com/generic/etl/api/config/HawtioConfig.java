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
 * Manual Hawtio servlet registration as a fallback when
 * hawtio-springboot auto-configuration doesn't kick in.
 * Serves the Hawtio console at /hawtio/.
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

            // Try multiple classpath locations for Hawtio resources
            InputStream in = findResource(resource);
            if (in == null) {
                resp.sendError(404, "Hawtio resource not found: " + resource);
                return;
            }

            String ext = resource.substring(resource.lastIndexOf('.') + 1).toLowerCase();
            resp.setContentType(CONTENT_TYPES.getOrDefault(ext, "application/octet-stream"));

            try (InputStream is = in) {
                StreamUtils.copy(is, resp.getOutputStream());
            }
        }

        private InputStream findResource(String resource) {
            ClassLoader cl = getClass().getClassLoader();
            InputStream in;
            in = cl.getResourceAsStream("hawtio" + resource);
            if (in != null) return in;
            in = cl.getResourceAsStream("static/hawtio" + resource);
            if (in != null) return in;
            in = cl.getResourceAsStream("META-INF/resources/hawtio" + resource);
            return in;
        }
    }
}
