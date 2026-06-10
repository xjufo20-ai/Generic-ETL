package com.generic.etl.common.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PipelineStatus {
    private String name;
    private String state;       // Started, Stopped, Suspended
    private long lastRunRows;
    private String lastRunAt;
    private String cron;
}
