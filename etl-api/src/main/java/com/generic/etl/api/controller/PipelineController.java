package com.generic.etl.api.controller;

import com.generic.etl.api.config.PipelineExecutionService;
import com.generic.etl.api.config.PipelineScheduler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.model.PipelineRun;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final PipelineExecutionService executionService;
    private final PipelineScheduler scheduler;
    private final Map<String, String> pipelineStore;

    public PipelineController(PipelineExecutionService executionService,
                               PipelineScheduler scheduler,
                               Map<String, String> pipelineStore) {
        this.executionService = executionService;
        this.scheduler = scheduler;
        this.pipelineStore = pipelineStore;
    }

    @PostMapping("/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> execute(@RequestBody String pipelineJson) {
        return ApiResponse.ok(executionService.executeFromJson(pipelineJson));
    }

    @PostMapping("/{name}/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> executeByName(@PathVariable String name) {
        return ApiResponse.ok(executionService.executeByName(name, pipelineStore));
    }

    @PostMapping("/{name}/retry")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRun> retry(@PathVariable String name, @RequestParam Long runId) {
        String json = pipelineStore.get(name);
        if (json == null) return ApiResponse.error("Pipeline not found: " + name);
        return ApiResponse.ok(executionService.retryRun(runId, json));
    }

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody String pipelineJson) {
        scheduler.register(pipelineJson);
        return ApiResponse.ok("Pipeline registered");
    }

    @DeleteMapping("/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> unregister(@PathVariable String name) {
        scheduler.unregister(name);
        return ApiResponse.ok("Pipeline '" + name + "' unregistered");
    }

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listPipelines() {
        return ApiResponse.ok(pipelineStore.keySet().stream().sorted().toList());
    }

    @GetMapping("/{name}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<String> getPipeline(@PathVariable String name) {
        String json = pipelineStore.get(name);
        if (json == null) return ApiResponse.error("Pipeline not found: " + name);
        return ApiResponse.ok(json);
    }

    @GetMapping("/runs")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<PipelineRun>> getRuns(@RequestParam(required = false) String pipeline) {
        return ApiResponse.ok(pipeline != null ? executionService.getRunHistory(pipeline) : executionService.getRunHistory());
    }

    @GetMapping("/runs/{runId}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<PipelineRun> getRun(@PathVariable Long runId) {
        PipelineRun run = executionService.getRun(runId);
        if (run == null) return ApiResponse.error("Run not found: " + runId);
        return ApiResponse.ok(run);
    }
}
