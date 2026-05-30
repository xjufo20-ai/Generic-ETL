package com.generic.etl.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class Row {
    private Map<String, Object> values;

    public Row() {
        this.values = new LinkedHashMap<>();
    }

    public Object get(String field) {
        return values.get(field);
    }

    public void put(String field, Object value) {
        values.put(field, value);
    }

    public void remove(String field) {
        values.remove(field);
    }

    public boolean has(String field) {
        return values.containsKey(field);
    }

    public Row copy() {
        return new Row(new LinkedHashMap<>(values));
    }
}
