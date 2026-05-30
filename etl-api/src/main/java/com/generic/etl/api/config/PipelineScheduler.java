package com.generic.etl.api.config;

import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.core.config.PipelineConfigParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Manages cron-based scheduling for registered pipelines.
 * When a pipeline is registered with a cron expression, it is scheduled
 * for recurring execution. Pipelines without a cron are not scheduled.
 */
public class PipelineScheduler {
    private static final Logger log = LoggerFactory.getLogger(PipelineScheduler.class);

    private final TaskScheduler taskScheduler;
    private final PipelineExecutionService executionService;
    private final PipelineConfigParser configParser;
    private final Map<String, String> pipelineStore;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    public PipelineScheduler(TaskScheduler taskScheduler,
                              PipelineExecutionService executionService,
                              PipelineConfigParser configParser,
                              Map<String, String> pipelineStore) {
        this.taskScheduler = taskScheduler;
        this.executionService = executionService;
        this.configParser = configParser;
        this.pipelineStore = pipelineStore;
    }

    /**
     * Register a pipeline and schedule it if it has a cron expression.
     */
    public void register(String pipelineJson) {
        try {
            PipelineConfig config = configParser.parseFromString(pipelineJson);
            String name = config.getPipeline().getName();
            pipelineStore.put(name, pipelineJson);

            if (config.getPipeline().getCron() != null && !config.getPipeline().getCron().isBlank()) {
                schedule(name, config.getPipeline().getCron());
            }

            log.info("Pipeline '{}' registered (cron: {})", name,
                    config.getPipeline().getCron() != null ? config.getPipeline().getCron() : "none");
        } catch (Exception e) {
            log.error("Failed to register pipeline", e);
            throw new RuntimeException("Failed to register pipeline: " + e.getMessage(), e);
        }
    }

    /**
     * Unregister a pipeline and cancel its schedule.
     */
    public void unregister(String name) {
        cancel(name);
        pipelineStore.remove(name);
        log.info("Pipeline '{}' unregistered", name);
    }

    private void schedule(String name, String cronExpression) {
        // Cancel existing schedule if any
        cancel(name);

        CronTrigger trigger = new CronTrigger(cronExpression, TimeZone.getDefault());
        ScheduledFuture<?> future = taskScheduler.schedule(
                () -> {
                    log.info("Cron trigger: executing pipeline '{}'", name);
                    try {
                        executionService.executeByName(name, pipelineStore);
                    } catch (Exception e) {
                        log.error("Scheduled pipeline '{}' failed", name, e);
                    }
                },
                trigger
        );

        scheduledTasks.put(name, future);
        log.info("Pipeline '{}' scheduled with cron: {}", name, cronExpression);
    }

    private void cancel(String name) {
        ScheduledFuture<?> future = scheduledTasks.remove(name);
        if (future != null) {
            future.cancel(false);
        }
    }

    public Map<String, String> getScheduledPipelines() {
        return Map.copyOf(scheduledTasks.keySet().stream()
                .collect(ConcurrentHashMap::new, (m, k) -> m.put(k, pipelineStore.get(k)), Map::putAll));
    }
}
