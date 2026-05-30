package com.generic.etl.common.model;

import lombok.Data;

@Data
public class JoinConfig {
    private String query;
    private String on;
    private String type; // INNER, LEFT
    private SchemaConfig joinSchema;
}
