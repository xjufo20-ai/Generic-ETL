package com.generic.etl.api.controller;

import com.generic.etl.api.config.PipelineExecutionService;
import com.generic.etl.api.config.PipelineScheduler;
import com.generic.etl.api.security.Roles;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.dto.PipelineRunResponse;
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

    // ── Execution (ADMIN, OPERATOR) ────────────────────────

    @PostMapping("/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRunResponse> execute(@RequestBody String pipelineJson) {
        PipelineRun run = executionService.executeFromJson(pipelineJson);
        return ApiResponse.ok(toResponse(run));
    }

    @PostMapping("/{name}/execute")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRunResponse> executeByName(@PathVariable String name) {
        PipelineRun run = executionService.executeByName(name, pipelineStore);
        return ApiResponse.ok(toResponse(run));
    }

    @PostMapping("/{name}/retry")
    @PreAuthorize(Roles.IS_ADMIN_OR_OPERATOR)
    public ApiResponse<PipelineRunResponse> retry(@PathVariable String name,
                                                   @RequestParam Long runId) {
        String json = pipelineStore.get(name);
        if (json == null) {
            return ApiResponse.error("Pipeline not found: " + name);
        }
        PipelineRun run = executionService.retryRun(runId, json);
        return ApiResponse.ok(toResponse(run));
    }

    // ── Pipeline config management (ADMIN only) ────────────

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

    // ── Read queries (all roles) ───────────────────────────

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listPipelines() {
        return ApiResponse.ok(pipelineStore.keySet().stream().sorted().toList());
    }

    @GetMapping("/{name}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<String> getPipeline(@PathVariable String name) {
        String json = pipelineStore.get(name);
        if (json == null) {
            return ApiResponse.error("Pipeline not found: " + name);
        }
        return ApiResponse.ok(json);
    }

    @GetMapping("/runs")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<PipelineRunResponse>> getRuns(
            @RequestParam(required = false) String pipeline) {
        List<PipelineRun> runs;
        if (pipeline != null) {
            runs = executionService.getRunHistory(pipeline);
        } else {
            runs = executionService.getRunHistory();
        }
        List<PipelineRunResponse> response = runs.stream()
                .map(PipelineController::toResponse)
                .toList();
        return ApiResponse.ok(response);
    }

    @GetMapping("/runs/{runId}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<PipelineRunResponse> getRun(@PathVariable Long runId) {
        PipelineRun run = executionService.getRun(runId);
        if (run == null) {
            return ApiResponse.error("Run not found: " + runId);
        }
        return ApiResponse.ok(toResponse(run));
    }

    private static PipelineRunResponse toResponse(PipelineRun run) {
        return PipelineRunResponse.builder()
                .id(run.getId())
                .pipelineName(run.getPipelineName())
                .status(run.getStatus())
                .rowCount(run.getRowCount())
                .durationMs(run.getDurationMs())
                .errorMessage(run.getErrorMessage())
                .startTime(run.getStartTime())
                .endTime(run.getEndTime())
                .build();
    }
}
