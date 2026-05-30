package com.generic.etl.common.model;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class PipelineRun {
    private Long id;
    private String pipelineName;
    private String status; // RUNNING, SUCCESS, FAILED
    private long rowCount;
    private long durationMs;
    private String errorMessage;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
