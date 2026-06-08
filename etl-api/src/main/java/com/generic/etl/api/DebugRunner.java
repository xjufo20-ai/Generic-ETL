package com.generic.etl.api;

import com.generic.etl.engine.ConsumerRegistry;
import com.generic.etl.engine.ResultCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class DebugRunner {
    private final ResultCache cache;
    private final ConsumerRegistry registry;

    @EventListener(ApplicationReadyEvent.class)
    public void debug() throws Exception {
        TimeUnit.SECONDS.sleep(8);
        log.info("=== Consumers: {}", registry.consumerNames());
        var rows = cache.get("test-minimal");
        log.info("=== test-minimal cache: {} rows", rows.size());
        if (!rows.isEmpty()) {
            log.info("=== first row: {}", rows.get(0).getValues());
        }
    }
}
