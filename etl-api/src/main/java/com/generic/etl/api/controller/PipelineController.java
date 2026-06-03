package com.generic.etl.api.controller;

import com.generic.etl.api.config.EtlYamlRouteLoader;
import com.generic.etl.api.security.Roles;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import org.apache.camel.CamelContext;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final CamelContext camelContext;
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;

    public PipelineController(CamelContext camelContext, StateStore store,
                               AuditLog auditLog, LineageStore lineageStore) {
        this.camelContext = camelContext;
        this.store = store;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
    }

    // ── YAML Route Management ────────────────────────────────

    @PostMapping("/yaml/load")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> loadYaml(@RequestBody String yaml) {
        try {
            String resolved = EtlYamlRouteLoader.resolveEnv(yaml);
            camelContext.getCamelContextExtension()
                .getContextPlugin(org.apache.camel.spi.RoutesLoader.class)
                .loadRoutes(camelContext, new org.apache.camel.spi.Resource() {
                    @Override public String getLocation() { return "inline"; }
                    @Override public java.io.InputStream getInputStream() {
                        return new java.io.ByteArrayInputStream(resolved.getBytes());
                    }
                });
            return ApiResponse.ok("YAML route loaded");
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @GetMapping("/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listRoutes() {
        return ApiResponse.ok(camelContext.getRoutes().stream()
                .map(r -> r.getRouteId()).sorted().toList());
    }

    @DeleteMapping("/routes/{routeId}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> removeRoute(@PathVariable String routeId) {
        try {
            camelContext.getRouteController().stopRoute(routeId);
            camelContext.removeRoute(routeId);
            return ApiResponse.ok("Route removed: " + routeId);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    // ── Pipeline Registry ────────────────────────────────────

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody Map<String, Object> pipeline) {
        String name = (String) pipeline.get("name");
        if (name == null) return ApiResponse.error("name is required");
        store.putPipeline(name, new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(pipeline));
        auditLog.recordChange(name, "REGISTER", "api");
        return ApiResponse.ok("Registered: " + name);
    }

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<Map<String, String>> listPipelines() {
        return ApiResponse.ok(store.getAllPipelines());
    }

    @GetMapping("/{name}/audit")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<AuditLog.Entry>> getAudit(@PathVariable String name) {
        return ApiResponse.ok(auditLog.getHistory(name));
    }

    @GetMapping("/lineage")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<Map<String,Object>>> getLineage(@RequestParam(required=false) String pipeline) {
        var entries = pipeline != null ? lineageStore.getByPipeline(pipeline) : lineageStore.getAll();
        return ApiResponse.ok(entries.stream().map(e -> Map.<String,Object>of(
            "pipeline",e.pipeline(),"outputTable",e.outputTable(),
            "consumer",e.consumer(),"rows",e.rows(),"status",e.status(),"timestamp",e.timestamp()
        )).toList());
    }
}
