package com.generic.etl.api.config;

import com.generic.etl.api.metrics.EtlMetrics;
import com.generic.etl.core.metrics.MetricsRecorder;
import com.generic.etl.core.store.AuditLog;
import com.generic.etl.core.store.LineageStore;
import com.generic.etl.engine.ConsumerRegistry;
import com.generic.etl.engine.LoadRouter;
import com.generic.etl.engine.ResultCache;
import com.generic.etl.engine.SqlResultNormalizer;
import com.generic.etl.engine.dispatch.ConsumerDispatchService;
import com.generic.etl.engine.persist.PersistHandler;
import jakarta.annotation.PostConstruct;
import org.apache.camel.CamelContext;
import org.apache.camel.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.client.RestClient;

import com.generic.etl.common.model.Row;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

@Configuration
public class LoadConfig {

    private final CamelContext camelContext;
    private final DataSource dataSource;

    public LoadConfig(CamelContext camelContext, DataSource dataSource) {
        this.camelContext = camelContext;
        this.dataSource = dataSource;
    }

    /** Bridge Spring's DataSource into Camel registry. */
    @PostConstruct
    public void initCamel() {
        camelContext.getRegistry().bind("etlDataSource", dataSource);
    }

    @Bean
    public PersistHandler persistHandler() {
        return new PersistHandler(dataSource);
    }

    /** Converts sql: component output (List<Map>) to List<Row>. */
    @Bean("rowConverter")
    public Processor rowConverter() {
        return exchange -> {
            Object body = exchange.getIn().getBody();
            if (body instanceof List<?> list) {
                exchange.getIn().setBody(list.stream()
                    .map(item -> item instanceof Row r ? r
                        : item instanceof Map<?,?> m ? new Row(new LinkedHashMap<>((Map<String, Object>) m))
                        : item)
                    .toList());
            } else if (body instanceof Map<?,?> m && !(body instanceof Row)) {
                exchange.getIn().setBody(new Row(new LinkedHashMap<>((Map<String, Object>) m)));
            }
        };
    }

    /**
     * Normalizes sql: producer output to List&lt;Map&lt;String, Object&gt;&gt;.
     * Inserted between sql: and transform steps to guarantee uniform body format.
     */
    @Bean("sqlListToMapList")
    public Processor sqlListToMapList() {
        return new SqlResultNormalizer();
    }

    @Bean
    public ConsumerRegistry consumerRegistry() { return new ConsumerRegistry(); }

    @Bean
    public ResultCache resultCache() { return new ResultCache(); }

    @Bean
    public ConsumerDispatchService consumerDispatchService(RestClient.Builder builder) {
        return new ConsumerDispatchService(builder);
    }

    @Bean
    @Primary
    public MetricsRecorder metricsRecorder(EtlMetrics etlMetrics) {
        return etlMetrics;
    }

    @Bean
    public LoadRouter loadRouter(ConsumerDispatchService dispatchService, ConsumerRegistry consumerRegistry,
                                  ResultCache resultCache, AuditLog auditLog,
                                  MetricsRecorder metrics, LineageStore lineageStore) {
        return new LoadRouter(dispatchService, consumerRegistry, resultCache, auditLog, metrics, lineageStore);
    }
}
