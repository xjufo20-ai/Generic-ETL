package com.generic.etl.common.model;

import lombok.Data;

/** Incremental extraction config. When set, only rows after the last watermark value are extracted. */
@Data
public class WatermarkConfig {
    private String column;    // timestamp or monotonically-increasing column
    private String initial;   // optional start value, e.g. "2024-01-01"
}
