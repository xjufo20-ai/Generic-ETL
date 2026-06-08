package com.generic.etl.api.controller;

import com.generic.etl.api.security.Roles;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.dto.ApiResponse;
import com.generic.etl.common.dto.DataResponse;
import com.generic.etl.common.model.ConsumerRegistration;
import com.generic.etl.common.model.Row;
import com.generic.etl.engine.ConsumerRegistry;
import com.generic.etl.engine.ResultCache;
import com.generic.etl.engine.dispatch.ConsumerDispatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/consumers")
@RequiredArgsConstructor
public class ConsumerController {
    private final ConsumerRegistry consumerRegistry;
    private final ConsumerDispatchService dispatchService;
    private final ResultCache inMemoryStore;
    private final StateStore store;

    @PostMapping("/register")
    @PreAuthorize(Roles.IS_ADMIN)
    public ApiResponse<String> register(@RequestBody ConsumerRegistration registration) {
        consumerRegistry.register(registration);
        store.addConsumer(registration);
        return ApiResponse.ok("Consumer '" + registration.getConsumer().getName() + "' registered");
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
        store.removeConsumer(name);
        return ApiResponse.ok("Consumer '" + name + "' unregistered");
    }

    @GetMapping("/data/{pipeline}")
    @PreAuthorize(Roles.IS_AUTHENTICATED)
    public ApiResponse<DataResponse> pullData(@PathVariable String pipeline, @RequestParam String consumer,
                                               @RequestParam(defaultValue = "0") int page,
                                               @RequestParam(defaultValue = "500") int pageSize) {
        List<Row> rows = inMemoryStore.get(pipeline);
        List<ConsumerRegistration.Subscription> subs = findSubscriptions(consumer, pipeline);
        if (subs.isEmpty()) return ApiResponse.error("No subscription for consumer '" + consumer + "' on '" + pipeline + "'");

        ConsumerRegistration.Subscription sub = subs.get(0);
        List<Map<String, Object>> projected = dispatchService.projectAndFilter(rows, sub);

        int totalPages = projected.isEmpty() ? 0 : (int) Math.ceil((double) projected.size() / pageSize);
        int from = Math.min(page * pageSize, projected.size());
        int to = Math.min(from + pageSize, projected.size());

        return ApiResponse.ok(DataResponse.builder().pipeline(pipeline).consumer(consumer)
                .totalRows(projected.size()).page(page).pageSize(pageSize).totalPages(totalPages)
                .data(projected.subList(from, to)).hasMore(page < totalPages - 1).build());
    }

    private List<ConsumerRegistration.Subscription> findSubscriptions(String consumerName, String pipeline) {
        List<ConsumerRegistration.Subscription> result = new ArrayList<>();
        for (ConsumerRegistration reg : consumerRegistry.getSubscriptions(consumerName))
            for (ConsumerRegistration.Subscription sub : reg.getSubscriptions())
                if (sub.getPipeline().equals(pipeline)) result.add(sub);
        return result;
    }
}
