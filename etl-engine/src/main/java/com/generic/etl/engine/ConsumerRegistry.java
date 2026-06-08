package com.generic.etl.engine;

import com.generic.etl.common.model.ConsumerRegistration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Thread-safe registry of consumers and their pipeline subscriptions. */
public class ConsumerRegistry {
    private final Map<String, List<ConsumerRegistration>> byConsumer = new ConcurrentHashMap<>();
    private final Map<String, List<ConsumerRegistration>> byPipeline = new ConcurrentHashMap<>();

    public void register(ConsumerRegistration registration) {
        String name = registration.getConsumer().getName();
        byConsumer.computeIfAbsent(name, k -> new CopyOnWriteArrayList<>()).add(registration);
        for (var sub : registration.getSubscriptions()) {
            byPipeline.computeIfAbsent(sub.getPipeline(), k -> new CopyOnWriteArrayList<>()).add(registration);
        }
    }

    public void unregister(String consumerName) {
        List<ConsumerRegistration> removed = byConsumer.remove(consumerName);
        if (removed != null) {
            for (var reg : removed) {
                for (var sub : reg.getSubscriptions()) {
                    byPipeline.getOrDefault(sub.getPipeline(), List.of()).remove(reg);
                }
            }
        }
    }

    public List<ConsumerRegistration> getByPipeline(String pipelineName) {
        return byPipeline.getOrDefault(pipelineName, List.of());
    }

    public List<String> consumerNames() {
        return new ArrayList<>(byConsumer.keySet());
    }

    public List<ConsumerRegistration> getSubscriptions(String consumerName) {
        return byConsumer.getOrDefault(consumerName, List.of());
    }
}
