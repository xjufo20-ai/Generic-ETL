package com.generic.etl.core.config;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.common.model.PipelineConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PipelineConfigParser {
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{env:([^}]+)\\}\\}");

    private final ObjectMapper mapper;

    public PipelineConfigParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public PipelineConfig parseFromFile(String filePath) throws IOException {
        String content = resolvePlaceholders(Files.readString(Path.of(filePath)));
        return mapper.readValue(content, PipelineConfig.class);
    }

    public PipelineConfig parseFromStream(InputStream stream) throws IOException {
        String content = resolvePlaceholders(new String(stream.readAllBytes()));
        return mapper.readValue(content, PipelineConfig.class);
    }

    public PipelineConfig parseFromString(String json) throws IOException {
        String content = resolvePlaceholders(json);
        return mapper.readValue(content, PipelineConfig.class);
    }

    static String resolvePlaceholders(String content) {
        Matcher m = PLACEHOLDER.matcher(content);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String envVar = m.group(1);
            String value = System.getenv(envVar);
            if (value == null) {
                log.warn("Environment variable {} not found, keeping placeholder", envVar);
                continue;
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
