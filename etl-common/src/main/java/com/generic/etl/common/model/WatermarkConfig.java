package com.generic.etl.common.model;

import lombok.Data;

@Data
public class WatermarkConfig {
    private String column;
    private String initial;
}
