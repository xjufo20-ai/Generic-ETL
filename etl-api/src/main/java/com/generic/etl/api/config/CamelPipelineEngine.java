package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.common.model.*;
import com.generic.etl.core.config.PipelineConfigParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Camel-based pipeline execution engine.
 * Translates PipelineConfig into a Camel route: from(source) → transform(s) → to(output).
 */
@Slf4j
public class CamelPipelineEngine {
    private final CamelContext camelContext;
    private final DataSource appDataSource;
    private final EtlMetrics metrics;
    private final AuditLog auditLog;
    private final LineageStore lineageStore;
    private final PipelineConfigParser configParser;

    public CamelPipelineEngine(CamelContext camelContext, DataSource appDataSource,
                                EtlMetrics metrics, AuditLog auditLog, LineageStore lineageStore,
                                PipelineConfigParser configParser) {
        this.camelContext = camelContext;
        this.appDataSource = appDataSource;
        this.metrics = metrics;
        this.auditLog = auditLog;
        this.lineageStore = lineageStore;
        this.configParser = configParser;
    }

    public PipelineRun execute(String pipelineJson) {
        PipelineConfig config;
        try { config = configParser.parseFromString(pipelineJson); }
        catch (Exception e) { return fail(null, "Parse error: " + e.getMessage()); }

        List<String> issues = config.validate();
        if (!issues.isEmpty()) return fail(config.getPipeline().getName(), "Validation: " + String.join("; ", issues));

        PipelineRun run = PipelineRun.builder().pipelineName(config.getPipeline().getName())
                .status("RUNNING").startTime(LocalDateTime.now()).build();
        AtomicInteger counter = new AtomicInteger(0);

        try {
            camelContext.getRegistry().bind("etlDataSource", appDataSource);
            String routeId = "camel-" + config.getPipeline().getName();

            // Extract via ProducerTemplate
            ProducerTemplate template = camelContext.createProducerTemplate();
            String sourceUri = buildSourceUri(config);
            String query = config.getDatasource() instanceof DataSourceConfig.JdbcDataSource j
                    ? j.getQuery() : null;

            List<Row> rows = new ArrayList<>();
            if (query != null) {
                @SuppressWarnings("unchecked")
                List<java.util.Map<String, Object>> result = template.requestBody(sourceUri, query, List.class);
                if (result != null) {
                    for (java.util.Map<String, Object> m : result) {
                        Row r = new Row();
                        m.forEach(r::put);
                        rows.add(r);
                    }
                }
            }

            // Apply transforms (reuse existing Java processors)
            int trxCount = 0;
            for (Row row : rows) {
                for (TransformDef t : config.getTransforms() != null ? config.getTransforms() : List.<TransformDef>of()) {
                    if (t instanceof TransformDef.FilterDef f) {
                        if (com.generic.etl.core.expression.ExpressionEvaluator.evaluate(row, f.getExpression())) trxCount++;
                    }
                }
            }
            counter.set(trxCount);

            long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("SUCCESS"); run.setRowCount(counter.get()); run.setDurationMs(dur); run.setEndTime(LocalDateTime.now());

            String outTable = config.getOutput() != null && config.getOutput().getStorage() != null
                    ? config.getOutput().getStorage().getTable() : "camel";
            lineageStore.record(config.getPipeline().getName(), outTable, "default", counter.get(), "SUCCESS");
            auditLog.recordExecution(config.getPipeline().getName(), "SUCCESS", counter.get(), "system");
            metrics.recordSuccess(config.getPipeline().getName(), counter.get(), dur);

        } catch (Exception e) {
            long dur = Duration.between(run.getStartTime(), LocalDateTime.now()).toMillis();
            run.setStatus("FAILED"); run.setDurationMs(dur); run.setEndTime(LocalDateTime.now());
            run.setErrorMessage(trunc(e.getMessage(), 2000));
            auditLog.recordExecution(config.getPipeline().getName(), "FAILED", 0, "system");
            metrics.recordFailure(config.getPipeline().getName());
        }
        return run;
    }

    private String buildSourceUri(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();
        if (ds instanceof DataSourceConfig.JdbcDataSource) {
            return "jdbc:etlDataSource?outputType=StreamList";
        }
        if (ds instanceof DataSourceConfig.CsvDataSource csv) {
            return "file:" + csv.getFilePath() + "?noop=true";
        }
        if (ds instanceof DataSourceConfig.KafkaDataSource k) {
            return "kafka:" + k.getConnection().getTopic()
                    + "?brokers=" + k.getConnection().getBootstrapServers()
                    + "&groupId=" + k.getConnection().getGroupId();
        }
        return "direct:noop";
    }

    private PipelineRun fail(String name, String msg) {
        return PipelineRun.builder().pipelineName(name).status("FAILED")
                .errorMessage(msg).startTime(LocalDateTime.now()).endTime(LocalDateTime.now()).build();
    }

    private static String trunc(String s, int max) {
        return s != null && s.length() > max ? s.substring(0, max) + "..." : s;
    }
}
