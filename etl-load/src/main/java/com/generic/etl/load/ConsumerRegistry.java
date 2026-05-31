package com.generic.etl.load;

import com.generic.etl.common.model.ConsumerRegistration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe registry of consumers and their pipeline subscriptions. */
public class ConsumerRegistry {
    private final Map<String, List<ConsumerRegistration>> registry = new ConcurrentHashMap<>();

    public void register(ConsumerRegistration registration) {
        String name = registration.getConsumer().getName();
        registry.computeIfAbsent(name, k -> new ArrayList<>()).add(registration);
    }

    public void unregister(String consumerName) {
        registry.remove(consumerName);
    }

    public List<ConsumerRegistration> getByPipeline(String pipelineName) {
        List<ConsumerRegistration> result = new ArrayList<>();
        for (List<ConsumerRegistration> regs : registry.values()) {
            for (ConsumerRegistration reg : regs) {
                for (ConsumerRegistration.Subscription sub : reg.getSubscriptions()) {
                    if (sub.getPipeline().equals(pipelineName)) {
                        result.add(reg);
                    }
                }
            }
        }
        return result;
    }

    public List<String> consumerNames() {
        return new ArrayList<>(registry.keySet());
    }

    public List<ConsumerRegistration> getSubscriptions(String consumerName) {
        return registry.getOrDefault(consumerName, List.of());
    }
}
