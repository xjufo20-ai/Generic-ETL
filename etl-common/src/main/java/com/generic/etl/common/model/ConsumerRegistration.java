package com.generic.etl.common.model;

import lombok.Data;
import java.util.List;

@Data
public class ConsumerRegistration {
    private Consumer consumer;
    private List<Subscription> subscriptions;

    @Data
    public static class Consumer {
        private String name;
        private String endpoint;
    }

    @Data
    public static class Subscription {
        private String pipeline;
        private SchemaConfig outputSchema;
        private String filter;
        private DeliveryConfig delivery;
    }

    @Data
    public static class DeliveryConfig {
        private String mode; // PULL, PUSH
        private int batchSize = 200;
    }
}
