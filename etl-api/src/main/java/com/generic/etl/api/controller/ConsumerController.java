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
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.*;

@Slf4j
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
        List<String> errors = validate(registration);
        if (!errors.isEmpty()) {
            return ApiResponse.error("Validation failed: " + String.join("; ", errors));
        }
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
        if (subs.isEmpty()) {
            return ApiResponse.error("No subscription for consumer '" + consumer + "' on '" + pipeline + "'");
        }

        ConsumerRegistration.Subscription sub = subs.get(0);
        List<Map<String, Object>> projected = dispatchService.projectAndFilter(rows, sub);

        int totalPages = projected.isEmpty() ? 0 : (int) Math.ceil((double) projected.size() / pageSize);
        int from = Math.min(page * pageSize, projected.size());
        int to = Math.min(from + pageSize, projected.size());

        return ApiResponse.ok(DataResponse.builder().pipeline(pipeline).consumer(consumer)
                .totalRows(projected.size()).page(page).pageSize(pageSize).totalPages(totalPages)
                .data(projected.subList(from, to)).hasMore(page < totalPages - 1).build());
    }

    // ── Validation ────────────────────────────────────────────────────

    private List<String> validate(ConsumerRegistration reg) {
        List<String> errors = new ArrayList<>();
        if (reg.getConsumer() == null || reg.getConsumer().getName() == null
                || reg.getConsumer().getName().isBlank()) {
            errors.add("consumer.name is required");
        }
        if (reg.getSubscriptions() == null || reg.getSubscriptions().isEmpty()) {
            errors.add("subscriptions is required");
        }
        for (int i = 0; i < (reg.getSubscriptions() != null ? reg.getSubscriptions().size() : 0); i++) {
            var sub = reg.getSubscriptions().get(i);
            String prefix = "subscriptions[" + i + "].";
            if (sub.getPipeline() == null || sub.getPipeline().isBlank()) {
                errors.add(prefix + "pipeline is required");
            }
            if (sub.getDelivery() != null && sub.getDelivery().getMode() != null) {
                String mode = sub.getDelivery().getMode().toUpperCase();
                if (!mode.equals("PULL") && !mode.equals("PUSH")) {
                    errors.add(prefix + "delivery.mode must be PULL or PUSH, got: " + mode);
                }
                // Normalize to uppercase
                sub.getDelivery().setMode(mode);
            }
        }
        // Validate endpoint for PUSH consumers
        if (reg.getConsumer() != null && reg.getConsumer().getEndpoint() != null
                && !reg.getConsumer().getEndpoint().isBlank()) {
            try {
                URI.create(reg.getConsumer().getEndpoint());
            } catch (IllegalArgumentException e) {
                errors.add("consumer.endpoint is not a valid URL: " + reg.getConsumer().getEndpoint());
            }
        }
        return errors;
    }

    private List<ConsumerRegistration.Subscription> findSubscriptions(String consumerName, String pipeline) {
        List<ConsumerRegistration.Subscription> result = new ArrayList<>();
        for (ConsumerRegistration reg : consumerRegistry.getSubscriptions(consumerName))
            for (ConsumerRegistration.Subscription sub : reg.getSubscriptions())
                if (sub.getPipeline().equals(pipeline)) result.add(sub);
        return result;
    }
}
