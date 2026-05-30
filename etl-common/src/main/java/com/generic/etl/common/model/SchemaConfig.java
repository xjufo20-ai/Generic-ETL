package com.generic.etl.common.model;

import lombok.Data;
import java.util.List;

@Data
public class SchemaConfig {
    private List<FieldDef> fields;

    @Data
    public static class FieldDef {
        private String name;
        private String type; // LONG, STRING, DECIMAL, DATETIME, BOOLEAN, INTEGER, DOUBLE
        private boolean nullable = true;
    }
}
