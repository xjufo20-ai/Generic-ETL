package com.generic.etl.load.dispatch;

import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.core.expression.ExpressionEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
public class ConsumerDispatchService {
    private final RestTemplate restTemplate = new RestTemplate();

    public void dispatch(String pipelineName, List<Row> rows, List<ConsumerRegistration> registrations) {
        for (ConsumerRegistration reg : registrations) {
            for (ConsumerRegistration.Subscription sub : reg.getSubscriptions()) {
                if (!sub.getPipeline().equals(pipelineName)) continue;
                List<Map<String, Object>> output = projectAndFilter(rows, sub);
                if (output.isEmpty()) continue;
                if ("PUSH".equalsIgnoreCase(sub.getDelivery() != null ? sub.getDelivery().getMode() : "PULL")) {
                    pushToConsumer(reg.getConsumer().getEndpoint(), output, reg.getConsumer().getName());
                }
            }
        }
    }

    public List<Map<String, Object>> projectAndFilter(List<Row> rows, ConsumerRegistration.Subscription sub) {
        List<String> fields = sub.getFields() != null ? sub.getFields() : List.of();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Row row : rows) {
            if (sub.getFilter() != null && !sub.getFilter().isBlank()) {
                if (!ExpressionEvaluator.evaluateMap(row.getValues(), sub.getFilter())) continue;
            }
            Map<String, Object> projected = new LinkedHashMap<>();
            if (fields.isEmpty()) {
                projected.putAll(row.getValues());
            } else {
                for (String field : fields) projected.put(field, row.get(field));
            }
            result.add(projected);
        }
        return result;
    }

    private void pushToConsumer(String endpoint, List<Map<String, Object>> data, String consumerName) {
        try {
            restTemplate.postForObject(endpoint, data, String.class);
            log.info("Pushed {} rows to consumer '{}'", data.size(), consumerName);
        } catch (Exception e) {
            log.error("Push failed to consumer '{}'", consumerName, e);
        }
    }
}
