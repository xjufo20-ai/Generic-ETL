package com.generic.etl.load.dispatch;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.SchemaConfig;
import com.generic.etl.core.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

public class ConsumerDispatchService {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDispatchService.class);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    /**
     * Dispatch transformed rows to all registered consumers for a pipeline.
     */
    public void dispatch(String pipelineName, List<Row> rows, List<ConsumerRegistration> registrations) {
        for (ConsumerRegistration reg : registrations) {
            for (ConsumerRegistration.Subscription sub : reg.getSubscriptions()) {
                if (!sub.getPipeline().equals(pipelineName)) continue;

                // Filter and project rows to match consumer's output schema
                List<Map<String, Object>> output = projectAndFilter(rows, sub);

                if (output.isEmpty()) continue;

                String mode = sub.getDelivery() != null ? sub.getDelivery().getMode() : "PULL";
                if ("PUSH".equalsIgnoreCase(mode)) {
                    pushToConsumer(reg.getConsumer().getEndpoint(), output, reg.getConsumer().getName());
                }
                // PULL mode: data stays in memory, consumer will pull via API
            }
        }
    }

    /**
     * Project rows to consumer's output schema and apply consumer-side filter.
     */
    public List<Map<String, Object>> projectAndFilter(List<Row> rows, ConsumerRegistration.Subscription sub) {
        SchemaConfig outputSchema = sub.getOutputSchema();
        List<String> fields = outputSchema.getFields().stream()
                .map(SchemaConfig.FieldDef::getName)
                .toList();

        List<Map<String, Object>> result = new ArrayList<>();

        for (Row row : rows) {
            // Apply consumer-side filter if present
            if (sub.getFilter() != null && !sub.getFilter().isBlank()) {
                if (!ExpressionEvaluator.evaluate(row, sub.getFilter())) {
                    continue;
                }
            }

            // Project to output schema
            Map<String, Object> projected = new LinkedHashMap<>();
            for (String field : fields) {
                projected.put(field, row.get(field));
            }
            result.add(projected);
        }

        return result;
    }

    private void pushToConsumer(String endpoint, List<Map<String, Object>> data, String consumerName) {
        try {
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(data);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                log.info("Pushed {} rows to consumer '{}' at {}", data.size(), consumerName, endpoint);
            } else {
                log.error("Failed to push to consumer '{}': HTTP {} - {}", consumerName, response.statusCode(), response.body());
            }
        } catch (Exception e) {
            log.error("Failed to push to consumer '{}' at {}", consumerName, endpoint, e);
        }
    }
}
