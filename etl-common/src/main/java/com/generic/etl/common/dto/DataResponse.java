package com.generic.etl.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataResponse {
    private String pipeline;
    private String consumer;
    private int totalRows;
    private int page;
    private int pageSize;
    private int totalPages;
    private List<Map<String, Object>> data;
    private String cursor; // for pagination
    private boolean hasMore;
}
