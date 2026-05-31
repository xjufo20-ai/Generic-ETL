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
    private final PipelineExecutionService executionService;
    private final PipelineConfigParser configParser;
    private final StateStore store;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    public PipelineScheduler(TaskScheduler taskScheduler, PipelineExecutionService executionService,
                              PipelineConfigParser configParser, StateStore store) {
        this.taskScheduler = taskScheduler;
        this.executionService = executionService;
        this.configParser = configParser;
        this.store = store;
    }

    public void register(String pipelineJson) throws Exception {
        PipelineConfig config;
        try { config = configParser.parseFromString(pipelineJson); } catch (Exception e) { throw new RuntimeException(e); }
        String name = config.getPipeline().getName();
        store.putPipeline(name, pipelineJson);
        // no throws here since StateStore handles internally

        if (config.getPipeline().getCron() != null && !config.getPipeline().getCron().isBlank()) {
            schedule(name, config.getPipeline().getCron());
        }
        log.info("Pipeline '{}' registered (cron: {})", name, config.getPipeline().getCron());
    }

    public void unregister(String name) {
        cancel(name);
        store.removePipeline(name);
        log.info("Pipeline '{}' unregistered", name);
    }

    private void schedule(String name, String cronExpression) {
        cancel(name);
        CronTrigger trigger = new CronTrigger(cronExpression, TimeZone.getDefault());
        ScheduledFuture<?> future = taskScheduler.schedule(() -> {
            log.info("Cron trigger: executing '{}'", name);
            try { executionService.executeByName(name, store); }
            catch (Exception e) { log.error("Scheduled '{}' failed", name, e); }
        }, trigger);
        scheduledTasks.put(name, future);
        log.info("Pipeline '{}' scheduled: {}", name, cronExpression);
    }

    private void cancel(String name) {
        ScheduledFuture<?> future = scheduledTasks.remove(name);
        if (future != null) future.cancel(false);
    }
}
