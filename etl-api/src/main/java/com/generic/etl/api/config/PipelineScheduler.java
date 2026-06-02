package com.generic.etl.api.config;

import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.config.PipelineConfigParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Slf4j
public class PipelineScheduler {

    private final TaskScheduler taskScheduler;
    private final CamelRouteFactory routeFactory;
    private final PipelineConfigParser configParser;
    private final StateStore store;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    public PipelineScheduler(TaskScheduler taskScheduler, CamelRouteFactory routeFactory,
                              PipelineConfigParser configParser, StateStore store) {
        this.taskScheduler = taskScheduler;
        this.routeFactory = routeFactory;
        this.configParser = configParser;
        this.store = store;
    }

    public void register(String pipelineJson) throws Exception {
        PipelineConfig config = configParser.parseFromString(pipelineJson);
        String name = config.getPipeline().getName();
        store.putPipeline(name, pipelineJson);

        // Register as Camel route
        routeFactory.register(config);

        // Schedule if cron is set
        if (config.getPipeline().getCron() != null && !config.getPipeline().getCron().isBlank()) {
            schedule(name, config.getPipeline().getCron());
        }
        log.info("Registered pipeline '{}'", name);
    }

    public void unregister(String name) {
        ScheduledFuture<?> f = scheduledTasks.remove(name);
        if (f != null) f.cancel(false);
        routeFactory.unregister(name);
        store.removePipeline(name);
        log.info("Unregistered pipeline '{}'", name);
    }

    private void schedule(String name, String cron) {
        CronTrigger trigger = new CronTrigger(cron, TimeZone.getDefault());
        ScheduledFuture<?> future = taskScheduler.schedule(() -> {
            try { routeFactory.execute(name); }
            catch (Exception e) { log.error("Scheduled pipeline '{}' failed", name, e); }
        }, trigger);
        scheduledTasks.put(name, future);
        log.info("Scheduled '{}' with cron: {}", name, cron);
    }
}
