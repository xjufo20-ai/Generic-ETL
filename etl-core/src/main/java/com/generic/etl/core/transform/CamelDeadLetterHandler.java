package com.generic.etl.core.transform;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Camel Dead Letter Channel handler.
 * Collects failed exchanges per pipeline for inspection.
 */
@Component("camelDeadLetterHandler")
@Slf4j
public class CamelDeadLetterHandler implements Processor {

    private final Map<String, List<String>> failures = new ConcurrentHashMap<>();

    @Override
    public void process(Exchange exchange) {
        String pipelineName = exchange.getProperty("pipelineName", "unknown", String.class);
        Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String errorMsg = cause != null ? cause.getMessage() : "Unknown error";

        failures.computeIfAbsent(pipelineName, k -> new CopyOnWriteArrayList<>()).add(errorMsg);
        log.error("Pipeline '{}' failed: {}", pipelineName, errorMsg);
    }

    /** Get failures for a pipeline. */
    public List<String> getFailures(String pipelineName) {
        return failures.getOrDefault(pipelineName, List.of());
    }

    /** Clear failures for a pipeline. */
    public void clear(String pipelineName) {
        failures.remove(pipelineName);
    }
}
