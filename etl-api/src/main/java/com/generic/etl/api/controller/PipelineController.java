package com.generic.etl.api.controller;

import com.generic.etl.api.config.CamelRouteFactory;
import com.generic.etl.api.config.PipelineScheduler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.config.PipelineConfigParser;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final CamelRouteFactory routeFactory;
    private final PipelineScheduler scheduler;
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;
    private final PipelineConfigParser configParser;

    public PipelineController(CamelRouteFactory routeFactory, PipelineScheduler scheduler,
                               StateStore store, AuditLog auditLog, LineageStore lineageStore,
                               PipelineConfigParser configParser) {
        this.routeFactory = routeFactory;
        this.scheduler = scheduler;
        this.store = store;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
        this.configParser = configParser;
    }

    // ── Register / Unregister ────────────────────────────────

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        try {
            scheduler.register(pipelineJson);
            return ApiResponse.ok("Pipeline registered");
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @DeleteMapping("/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> unregister(@PathVariable String name) {
        scheduler.unregister(name);
        return ApiResponse.ok("Pipeline '" + name + "' unregistered");
    }

    // ── Execute ───────────────────────────────────────────────

    @PostMapping("/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<String> execute(@RequestBody String pipelineJson) {
        try {
            PipelineConfig config = configParser.parseFromString(pipelineJson);
            String name = config.getPipeline().getName();
            // Register if not already, then trigger
            if (!routeFactory.isRegistered(name)) routeFactory.register(config);
            routeFactory.execute(name);
            return ApiResponse.ok("Executed: " + name);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    @PostMapping("/{name}/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<String> executeByName(@PathVariable String name) {
        try {
            routeFactory.execute(name);
            return ApiResponse.ok("Executed: " + name);
        } catch (Exception e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    // ── Query ─────────────────────────────────────────────────

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listPipelines() {
        return ApiResponse.ok(store.getAllPipelines().keySet().stream().sorted().toList());
    }

    @GetMapping("/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<Set<String>> listRoutes() {
        return ApiResponse.ok(routeFactory.getRegisteredPipelines());
    }

    @GetMapping("/{name}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<String> getPipeline(@PathVariable String name) {
        String json = store.getPipeline(name);
        if (json == null) return ApiResponse.error("Not found: " + name);
        return ApiResponse.ok(json);
    }

    @GetMapping("/{name}/audit")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<AuditLog.Entry>> getAudit(@PathVariable String name) {
        return ApiResponse.ok(auditLog.getHistory(name));
    }

    @GetMapping("/lineage")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<Map<String,Object>>> getLineage(@RequestParam(required=false) String pipeline) {
        List<LineageStore.LineageEntry> entries = pipeline != null ? lineageStore.getByPipeline(pipeline) : lineageStore.getAll();
        return ApiResponse.ok(entries.stream().map(e -> Map.<String,Object>of(
            "pipeline",e.pipeline(),"outputTable",e.outputTable(),
            "consumer",e.consumer(),"rows",e.rows(),
            "status",e.status(),"timestamp",e.timestamp()
        )).toList());
    }
}
