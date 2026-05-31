package com.generic.etl.api.controller;

import com.generic.etl.api.security.Roles;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.dto.DataResponse;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.load.ConsumerRegistry;
import com.generic.etl.load.InMemoryDataStore;
import com.generic.etl.load.dispatch.ConsumerDispatchService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/api/consumers")
public class ConsumerController {
    private final ConsumerRegistry consumerRegistry;
    private final ConsumerDispatchService dispatchService;
    private final InMemoryDataStore inMemoryStore;

    public ConsumerController(ConsumerRegistry consumerRegistry,
                               ConsumerDispatchService dispatchService,
                               InMemoryDataStore inMemoryStore) {
        this.consumerRegistry = consumerRegistry;
        this.dispatchService = dispatchService;
        this.inMemoryStore = inMemoryStore;
    }

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody ConsumerRegistration registration) {
        String consumerName = registration.getConsumer().getName();
        consumerRegistry.register(registration);
        return ApiResponse.ok("Consumer '" + consumerName + "' registered with " +
                registration.getSubscriptions().size() + " subscription(s)");
    }

    @GetMapping
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<List<String>> listConsumers() {
        return ApiResponse.ok(consumerRegistry.consumerNames());
    }

    @DeleteMapping("/{name}")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> unregister(@PathVariable String name) {
        consumerRegistry.unregister(name);
        return ApiResponse.ok("Consumer '" + name + "' unregistered");
    }

    @GetMapping("/data/{pipeline}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<DataResponse> pullData(
            @PathVariable String pipeline,
            @RequestParam String consumer,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "500") int pageSize) {

        List<Row> rows = inMemoryStore.get(pipeline);
        List<ConsumerRegistration.Subscription> subs = findSubscriptions(consumer, pipeline);

        if (subs.isEmpty()) {
            return ApiResponse.error("No subscription found for consumer '" + consumer +
                    "' on pipeline '" + pipeline + "'");
        }

        ConsumerRegistration.Subscription sub = subs.get(0);
        List<Map<String, Object>> projected = dispatchService.projectAndFilter(rows, sub);

        int totalPages = projected.isEmpty() ? 0 : (int) Math.ceil((double) projected.size() / pageSize);
        int from = Math.min(page * pageSize, projected.size());
        int to = Math.min(from + pageSize, projected.size());
        List<Map<String, Object>> pageData = projected.subList(from, to);

        DataResponse response = DataResponse.builder()
                .pipeline(pipeline)
                .consumer(consumer)
                .totalRows(projected.size())
                .page(page)
                .pageSize(pageSize)
                .totalPages(totalPages)
                .data(pageData)
                .hasMore(page < totalPages - 1)
                .build();

        return ApiResponse.ok(response);
    }

    private List<ConsumerRegistration.Subscription> findSubscriptions(String consumerName, String pipeline) {
        List<ConsumerRegistration> regs = consumerRegistry.getSubscriptions(consumerName);
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
