package com.generic.etl.common.model;

import lombok.Data;
import java.util.List;

/** Minimal pipeline metadata for consumer reference. Route definition lives in Camel YAML DSL. */
@Data
public class PipelineConfig {
    private String name;
    private String version;
    private String cron;         // optional: Spring cron schedule
    private List<String> outputFields;  // what this pipeline produces (for consumer validation)
}
