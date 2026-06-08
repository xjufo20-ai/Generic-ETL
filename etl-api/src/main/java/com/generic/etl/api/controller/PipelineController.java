package com.generic.etl.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.api.config.EtlYamlRouteLoader;
import com.generic.etl.core.compile.JsonToYamlCompiler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.core.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.config.PipelineConfigValidator;
import lombok.RequiredArgsConstructor;
import org.apache.camel.CamelContext;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/pipelines")
@RequiredArgsConstructor
public class PipelineController {
    private final CamelContext camelContext;
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;
    private final EtlYamlRouteLoader yamlLoader;
    private final ObjectMapper mapper;
    private final PipelineConfigValidator validator = new PipelineConfigValidator();

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        try {
            PipelineConfig config = mapper.readValue(
                    EtlYamlRouteLoader.resolveEnv(pipelineJson), PipelineConfig.class);
            List<String> issues = validator.validate(config);
            if (!issues.isEmpty()) return ApiResponse.error("Validation failed: " + String.join("; ", issues));
            String yaml = JsonToYamlCompiler.compile(config);
            yamlLoader.loadYaml(yaml, config.getPipeline().getName() + ".yaml");
            store.putPipeline(config.getPipeline().getName(), pipelineJson);
            auditLog.recordChange(config.getPipeline().getName(), "REGISTER", "api");
            return ApiResponse.ok("Registered: " + config.getPipeline().getName());
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @PostMapping("/yaml")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> loadYaml(@RequestBody String yaml) {
        try {
            yamlLoader.loadYaml(EtlYamlRouteLoader.resolveEnv(yaml), "inline.yaml");
            return ApiResponse.ok("YAML loaded");
        } catch (Exception e) { return ApiResponse.error(e.getMessage()); }
    }

    @GetMapping("/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listRoutes() {
        return ApiResponse.ok(camelContext.getRoutes().stream().map(r -> r.getRouteId()).sorted().toList());
    }

    @DeleteMapping("/routes/{routeId}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> removeRoute(@PathVariable String routeId) {
        try {
            camelContext.getRouteController().stopRoute(routeId);
            camelContext.removeRoute(routeId);
            return ApiResponse.ok("Removed: " + routeId);
        } catch (Exception e) { return ApiResponse.error(e.getMessage()); }
    }

    @GetMapping
    public ApiResponse<Map<String, String>> list() { return ApiResponse.ok(store.getAllPipelines()); }

    @GetMapping("/{name}/audit")
    public ApiResponse<List<AuditLog.Entry>> audit(@PathVariable String name) {
        return ApiResponse.ok(auditLog.getHistory(name));
    }

    @GetMapping("/lineage")
    public ApiResponse<List<Map<String, Object>>> lineage(@RequestParam(required = false) String pipeline) {
        var entries = pipeline != null ? lineageStore.getByPipeline(pipeline) : lineageStore.getAll();
        return ApiResponse.ok(entries.stream().map(e -> Map.<String, Object>of(
                "pipeline", e.pipeline(), "outputTable", e.outputTable(),
                "consumer", e.consumer(), "rows", e.rows(),
                "status", e.status(), "timestamp", e.timestamp())).toList());
    }
}
