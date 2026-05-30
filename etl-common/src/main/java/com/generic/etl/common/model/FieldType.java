package com.generic.etl.common.model;

public enum FieldType {
    LONG, INTEGER, STRING, DECIMAL, DOUBLE, DATETIME, BOOLEAN;

    public static FieldType fromString(String type) {
        for (FieldType ft : values()) {
            if (ft.name().equalsIgnoreCase(type)) {
                return ft;
            }
        }
        return STRING; // default
    }
}
