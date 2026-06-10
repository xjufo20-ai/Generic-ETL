package com.generic.etl.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.api.config.EtlYamlRouteLoader;
import com.generic.etl.core.compile.JsonToYamlCompiler;
import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.security.Roles;
import com.generic.etl.core.store.AuditLog;
import com.generic.etl.core.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.dto.PipelineStatus;
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
    private final EtlMetrics metrics;
    private final EtlYamlRouteLoader yamlLoader;
    private final ObjectMapper mapper;
    private final PipelineConfigValidator validator = new PipelineConfigValidator();

    // ── Lifecycle ──────────────────────────────────────────────────

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        try {
            PipelineConfig config = mapper.readValue(
                    EtlYamlRouteLoader.resolveEnv(pipelineJson), PipelineConfig.class);
            List<String> issues = validator.validate(config);
            if (!issues.isEmpty()) {
                return ApiResponse.error("Validation failed: " + String.join("; ", issues));
            }

            String name = config.getPipeline().getName();
            String yaml = JsonToYamlCompiler.compile(config);
            yamlLoader.loadYaml(yaml, name + ".yaml");
            store.putPipeline(name, pipelineJson);
            auditLog.recordChange(name, "REGISTER", "api");

            return ApiResponse.ok("Registered: " + name);
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
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @DeleteMapping("/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> remove(@PathVariable String name) {
        try {
            camelContext.getRouteController().stopRoute(name);
            camelContext.removeRoute(name);
            store.removePipeline(name);
            auditLog.recordChange(name, "DELETE", "api");
            return ApiResponse.ok("Removed: " + name);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @PostMapping("/{name}/suspend")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<String> suspend(@PathVariable String name) {
        try {
            camelContext.getRouteController().stopRoute(name);
            return ApiResponse.ok("Suspended: " + name);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @PostMapping("/{name}/resume")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<String> resume(@PathVariable String name) {
        try {
            camelContext.getRouteController().startRoute(name);
            return ApiResponse.ok("Resumed: " + name);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    // ── Query ──────────────────────────────────────────────────────

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<PipelineStatus>> list() {
        List<PipelineStatus> statuses = new ArrayList<>();
        for (var route : camelContext.getRoutes()) {
            String name = route.getRouteId();
            String state = camelContext.getRouteController().getRouteStatus(name).name();
            var lastAudit = auditLog.getHistory(name).stream()
                    .filter(e -> "EXECUTION".equals(e.action()))
                    .findFirst();
            statuses.add(PipelineStatus.builder()
                    .name(name)
                    .state(state)
                    .lastRunRows(lastAudit.map(AuditLog.Entry::rows).orElse(0L))
                    .lastRunAt(lastAudit.map(AuditLog.Entry::timestamp).orElse(null))
                    .build());
        }
        return ApiResponse.ok(statuses);
    }

    @GetMapping("/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listRoutes() {
        return ApiResponse.ok(camelContext.getRoutes().stream()
                .map(r -> r.getRouteId()).sorted().toList());
    }

    @GetMapping("/{name}/audit")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<Map<String, Object>> audit(@PathVariable String name,
                                                   @RequestParam(defaultValue = "0") int page,
                                                   @RequestParam(defaultValue = "50") int size) {
        List<AuditLog.Entry> all = auditLog.getHistory(name);
        int total = all.size();
        int from = Math.min(page * size, total);
        int to = Math.min(from + size, total);
        return ApiResponse.ok(Map.of(
                "data", all.subList(from, to),
                "page", page,
                "size", size,
                "total", total,
                "totalPages", total == 0 ? 0 : (int) Math.ceil((double) total / size)
        ));
    }

    @GetMapping("/lineage")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<Map<String, Object>>> lineage(
            @RequestParam(required = false) String pipeline) {
        var entries = pipeline != null
                ? lineageStore.getByPipeline(pipeline)
                : lineageStore.getAll();
        return ApiResponse.ok(entries.stream().map(e -> Map.<String, Object>of(
                "pipeline", e.pipeline(),
                "outputTable", e.outputTable(),
                "consumer", e.consumer(),
                "rows", e.rows(),
                "status", e.status(),
                "timestamp", e.timestamp()
        )).toList());
    }
}
