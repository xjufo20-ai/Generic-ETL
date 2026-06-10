package com.generic.etl.core.compile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared YAML escaping and JSON serialization utilities for step compilers.
 */
public final class YamlUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private YamlUtils() {}

    /** Escape a value for single-quoted YAML strings. */
    public static String escapeYamlSingleQuote(String s) {
        return s.replace("'", "''");
    }

    /** Escape a value for double-quoted YAML strings. */
    public static String escapeYamlDoubleQuote(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Quote a YAML scalar value using single quotes when needed.
     */
    public static String quoteYaml(String s) {
        if (s == null || s.isEmpty()) return "''";
        if (s.contains(":") || s.contains("#") || s.contains("{") || s.contains("}")
                || s.contains("[") || s.contains("]") || s.contains(",")
                || s.contains("&") || s.contains("*") || s.contains("?")
                || s.contains("|") || s.contains(">") || s.contains("!")
                || s.contains("%") || s.contains("@") || s.contains("`")
                || s.contains("\"") || s.contains("'")
                || s.startsWith(" ") || s.endsWith(" ")
                || s.startsWith("-") || s.startsWith(".")
                || s.contains("\n") || s.contains("\r")) {
            return "'" + escapeYamlSingleQuote(s) + "'";
        }
        return s;
    }

    /** Serialize an object to a compact JSON string. */
    public static String toJson(Object obj) {
        try { return MAPPER.writeValueAsString(obj); }
        catch (JsonProcessingException e) { return "{}"; }
    }
}
