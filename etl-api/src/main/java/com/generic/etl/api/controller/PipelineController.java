package com.generic.etl.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.api.config.EtlYamlRouteLoader;
import com.generic.etl.api.config.JsonToYamlCompiler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.model.PipelineConfig;
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
    private final EtlYamlRouteLoader yamlLoader;
    private final ObjectMapper mapper;

    public PipelineController(CamelContext camelContext, StateStore store, AuditLog auditLog,
                               LineageStore lineageStore, EtlYamlRouteLoader yamlLoader, ObjectMapper mapper) {
        this.camelContext = camelContext; this.store = store; this.auditLog = auditLog;
        this.lineageStore = lineageStore; this.yamlLoader = yamlLoader; this.mapper = mapper;
    }

    // ── JSON Pipeline (compile → YAML → Camel) ─────────────

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        try {
            PipelineConfig config = mapper.readValue(
                    EtlYamlRouteLoader.resolveEnv(pipelineJson), PipelineConfig.class);
            String yaml = JsonToYamlCompiler.compile(config);
            yamlLoader.loadYaml(yaml, config.getPipeline().getName());
            store.putPipeline(config.getPipeline().getName(), pipelineJson);
            auditLog.recordChange(config.getPipeline().getName(), "REGISTER", "api");
            return ApiResponse.ok("Registered: " + config.getPipeline().getName());
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    // ── YAML direct ────────────────────────────────────────

    @PostMapping("/yaml")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> loadYaml(@RequestBody String yaml) {
        try {
            yamlLoader.loadYaml(EtlYamlRouteLoader.resolveEnv(yaml), "inline");
            return ApiResponse.ok("YAML loaded");
        } catch (Exception e) { return ApiResponse.error(e.getMessage()); }
    }

    // ── Query ──────────────────────────────────────────────

    @GetMapping("/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listRoutes() {
        return ApiResponse.ok(camelContext.getRoutes().stream().map(r -> r.getRouteId()).sorted().toList());
    }

    @DeleteMapping("/routes/{routeId}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> removeRoute(@PathVariable String routeId) {
        try { camelContext.getRouteController().stopRoute(routeId); camelContext.removeRoute(routeId);
            return ApiResponse.ok("Removed: " + routeId); }
        catch (Exception e) { return ApiResponse.error(e.getMessage()); }
    }

    @GetMapping public ApiResponse<Map<String,String>> list() { return ApiResponse.ok(store.getAllPipelines()); }

    @GetMapping("/{name}/audit")
    public ApiResponse<List<AuditLog.Entry>> audit(@PathVariable String name) { return ApiResponse.ok(auditLog.getHistory(name)); }

    @GetMapping("/lineage")
    public ApiResponse<List<Map<String,Object>>> lineage(@RequestParam(required=false) String pipeline) {
        var e = pipeline != null ? lineageStore.getByPipeline(pipeline) : lineageStore.getAll();
        return ApiResponse.ok(e.stream().map(x -> Map.<String,Object>of("pipeline",x.pipeline(),"outputTable",x.outputTable(),"consumer",x.consumer(),"rows",x.rows(),"status",x.status(),"timestamp",x.timestamp())).toList());
    }
}
