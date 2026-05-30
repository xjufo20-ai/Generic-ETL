package com.generic.etl.transform.impl;

import com.generic.etl.common.model.FieldType;
import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import com.generic.etl.core.transform.TransformProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class TypeCastProcessor implements TransformProcessor {

    @Override
    public Row process(Row row, TransformDef def) {
        if (def.getMappings() == null) return row;
        for (TransformDef.MappingDef mapping : def.getMappings()) {
            if (!row.has(mapping.getField())) continue;
            Object value = row.get(mapping.getField());
            FieldType targetType = FieldType.fromString(mapping.getToType());
            row.put(mapping.getField(), cast(value, targetType));
        }
        return row;
    }

    private Object cast(Object value, FieldType targetType) {
        if (value == null) return null;
        String str = value.toString().trim();
        try {
            return switch (targetType) {
                case LONG -> Long.parseLong(str);
                case INTEGER -> Integer.parseInt(str);
                case STRING -> str;
                case DECIMAL -> new BigDecimal(str).setScale(4, RoundingMode.HALF_UP);
                case DOUBLE -> Double.parseDouble(str);
                case BOOLEAN -> Boolean.parseBoolean(str);
                case DATETIME -> {
                    try {
                        yield LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    } catch (DateTimeParseException e) {
                        yield LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to cast '" + str + "' to " + targetType, e);
        }
    }
}
