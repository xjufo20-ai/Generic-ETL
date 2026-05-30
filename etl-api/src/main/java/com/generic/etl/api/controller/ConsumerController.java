package com.generic.etl.api.controller;

import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.dto.DataResponse;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.InMemoryDataStore;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consumers")
public class ConsumerController {
    private final Map<String, List<ConsumerRegistration>> consumerRegistry;
    private final ConsumerDispatchService dispatchService;

    public ConsumerController(Map<String, List<ConsumerRegistration>> consumerRegistry,
                               ConsumerDispatchService dispatchService) {
        this.consumerRegistry = consumerRegistry;
        this.dispatchService = dispatchService;
    }

    @PostMapping("/register")
    public ApiResponse<String> register(@RequestBody ConsumerRegistration registration) {
        String consumerName = registration.getConsumer().getName();
        consumerRegistry.computeIfAbsent(consumerName, k -> new ArrayList<>()).add(registration);
        return ApiResponse.ok("Consumer '" + consumerName + "' registered with " +
                registration.getSubscriptions().size() + " subscription(s)");
    }

    @GetMapping
    public ApiResponse<List<String>> listConsumers() {
        return ApiResponse.ok(new ArrayList<>(consumerRegistry.keySet()));
    }

    @DeleteMapping("/{name}")
    public ApiResponse<String> unregister(@PathVariable String name) {
        consumerRegistry.remove(name);
        return ApiResponse.ok("Consumer '" + name + "' unregistered");
    }

    /** PULL: downstream fetches data for a pipeline. */
    @GetMapping("/data/{pipeline}")
    public ApiResponse<DataResponse> pullData(
            @PathVariable String pipeline,
            @RequestParam String consumer) {

        List<Row> rows = InMemoryDataStore.get(pipeline);
        List<ConsumerRegistration.Subscription> subs = findSubscriptions(consumer, pipeline);

        if (subs.isEmpty()) {
            return ApiResponse.error("No subscription found for consumer '" + consumer +
                    "' on pipeline '" + pipeline + "'");
        }

        // Use first subscription's output schema
        ConsumerRegistration.Subscription sub = subs.get(0);
        List<Map<String, Object>> projected = dispatchService.projectAndFilter(rows, sub);

        DataResponse response = DataResponse.builder()
                .pipeline(pipeline)
                .consumer(consumer)
                .totalRows(projected.size())
                .data(projected)
                .hasMore(false)
                .build();

        return ApiResponse.ok(response);
    }

    private List<ConsumerRegistration.Subscription> findSubscriptions(String consumerName, String pipeline) {
        List<ConsumerRegistration> regs = consumerRegistry.getOrDefault(consumerName, List.of());
        List<ConsumerRegistration.Subscription> result = new ArrayList<>();
        for (ConsumerRegistration reg : regs) {
            for (ConsumerRegistration.Subscription sub : reg.getSubscriptions()) {
                if (sub.getPipeline().equals(pipeline)) {
                    result.add(sub);
                }
            }
        }
        return result;
    }
}
