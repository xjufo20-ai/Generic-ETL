package com.generic.etl.api.controller;

import com.generic.etl.api.config.CamelRouteFactory;
import com.generic.etl.api.config.PipelineExecutionService;
import com.generic.etl.api.config.PipelineScheduler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.PipelineRun;
import com.generic.etl.core.config.PipelineConfigParser;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final PipelineExecutionService executionService;
    private final PipelineScheduler scheduler;
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;
    private final CamelRouteFactory camelRouteFactory;
    private final PipelineConfigParser configParser;

    public PipelineController(PipelineExecutionService executionService,
                               PipelineScheduler scheduler, StateStore store,
                               AuditLog auditLog, LineageStore lineageStore,
                               CamelRouteFactory camelRouteFactory,
                               PipelineConfigParser configParser) {
        this.executionService = executionService;
        this.scheduler = scheduler;
        this.store = store;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
        this.camelRouteFactory = camelRouteFactory;
        this.configParser = configParser;
    }

    // ── Execution ─────────────────────────────────────────────

    @PostMapping("/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> execute(@RequestBody String pipelineJson) {
        return ApiResponse.ok(executionService.executeFromJson(pipelineJson));
    }

    @PostMapping("/{name}/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> executeByName(@PathVariable String name) {
        return ApiResponse.ok(executionService.executeByName(name, store));
    }

    @PostMapping("/{name}/retry")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> retry(@PathVariable String name, @RequestParam Long runId) {
        String json = store.getPipeline(name);
        if (json == null) return ApiResponse.error("Pipeline not found: " + name);
        return ApiResponse.ok(executionService.retryRun(runId, json));
    }

    // ── Registration ──────────────────────────────────────────

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        try { scheduler.register(pipelineJson); } catch (Exception e) { return ApiResponse.error(e.getMessage()); }
        return ApiResponse.ok("Pipeline registered");
    }

    @DeleteMapping("/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> unregister(@PathVariable String name) {
        scheduler.unregister(name);
        camelRouteFactory.unregister(name);
        return ApiResponse.ok("Pipeline '" + name + "' unregistered");
    }

    // ── Camel Route Management ────────────────────────────────

    @PostMapping("/camel/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> registerCamel(@RequestBody String pipelineJson) {
        try {
            PipelineConfig config = configParser.parseFromString(pipelineJson);
            camelRouteFactory.register(config);
            return ApiResponse.ok("Camel route registered: " + config.getPipeline().getName());
        } catch (Exception e) {
            return ApiResponse.error("Failed: " + e.getMessage());
        }
    }

    @DeleteMapping("/camel/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> unregisterCamel(@PathVariable String name) {
        camelRouteFactory.unregister(name);
        return ApiResponse.ok("Camel route removed: " + name);
    }

    @GetMapping("/camel/routes")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<Set<String>> listCamelRoutes() {
        return ApiResponse.ok(camelRouteFactory.getRegisteredPipelines());
    }

    // ── Query ─────────────────────────────────────────────────

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listPipelines() {
        return ApiResponse.ok(store.getAllPipelines().keySet().stream().sorted().toList());
    }

    @GetMapping("/{name}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<String> getPipeline(@PathVariable String name) {
        String json = store.getPipeline(name);
        if (json == null) return ApiResponse.error("Pipeline not found: " + name);
        return ApiResponse.ok(json);
    }

    @GetMapping("/runs")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<PipelineRun>> getRuns(@RequestParam(required = false) String pipeline) {
        return ApiResponse.ok(pipeline != null ? executionService.getRunHistory(pipeline) : executionService.getRunHistory());
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

    @GetMapping("/runs/{runId}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<PipelineRun> getRun(@PathVariable Long runId) {
        PipelineRun run = executionService.getRun(runId);
        if (run == null) return ApiResponse.error("Run not found: " + runId);
        return ApiResponse.ok(run);
    }
}
