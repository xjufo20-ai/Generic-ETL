package com.generic.etl.common.model;

import lombok.Data;

@Data
public class ParallelConfig {
    private int extractPartitions = 1;   // parallel extraction shards
    private int transformThreads = 1;     // parallel transform workers
    private int pageSize = 5000;          // rows per batch in parallel mode
}
