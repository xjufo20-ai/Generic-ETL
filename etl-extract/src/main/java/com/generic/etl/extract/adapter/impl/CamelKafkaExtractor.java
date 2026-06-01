package com.generic.etl.extract.adapter.impl;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.extract.adapter.Extractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

@Slf4j
public class CamelKafkaExtractor implements Extractor {
    private final CamelContext camelContext;

    public CamelKafkaExtractor(CamelContext camelContext) { this.camelContext = camelContext; }

    @Override public boolean supports(PipelineConfig config) { return config.getDatasource() instanceof DataSourceConfig.KafkaDataSource; }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig.KafkaDataSource ds = (DataSourceConfig.KafkaDataSource) config.getDatasource();
        String uri = String.format("kafka:%s?brokers=%s&groupId=%s&autoOffsetReset=earliest&maxPollRecords=500",
                ds.getConnection().getTopic(), ds.getConnection().getBootstrapServers(), ds.getConnection().getGroupId());
        List<Map<String, Object>> messages = new CopyOnWriteArrayList<>();
        try {
            String routeId = "kafka-extract-" + ds.getConnection().getTopic();
            camelContext.addRoutes(new RouteBuilder() {
                @Override public void configure() {
                    from(uri).routeId(routeId).process(exchange -> {
                        String body = exchange.getIn().getBody(String.class);
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("kafka_topic", ds.getConnection().getTopic());
                        row.put("kafka_offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
                        row.put("value", body);
                        messages.add(row);
                    }).to("log:kafka?level=DEBUG");
                }
            });
            Thread.sleep(5000);
            camelContext.getRouteController().stopRoute(routeId);
            camelContext.removeRoute(routeId);
        } catch (Exception e) { throw new RuntimeException("Kafka extraction failed", e); }
        return messages.stream().map(Row::new);
    }
}
