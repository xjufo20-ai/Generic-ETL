package com.generic.etl.extract.adapter.impl;

import com.generic.etl.common.model.DataSourceConfig;
import com.generic.etl.common.model.PipelineConfig;
import com.generic.etl.common.model.Row;
import com.generic.etl.extract.adapter.Extractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class CamelSftpExtractor implements Extractor {
    private final CamelContext camelContext;

    public CamelSftpExtractor(CamelContext camelContext) { this.camelContext = camelContext; }

    @Override public boolean supports(PipelineConfig config) { return config.getDatasource() instanceof DataSourceConfig.SftpDataSource; }

    @Override
    public Stream<Row> extract(PipelineConfig config) {
        DataSourceConfig.SftpDataSource ds = (DataSourceConfig.SftpDataSource) config.getDatasource();
        String uri = String.format("sftp://%s@%s:%d%s?password=%s&fileName=%s&noop=true",
                ds.getConnection().getUsername(), ds.getConnection().getHost(),
                ds.getConnection().getPort(), ds.getConnection().getDirectory(),
                ds.getConnection().getPassword(), ds.getFileName());
        List<Map<String, Object>> rows = new ArrayList<>();
        String routeId = "sftp-extract-" + ds.getConnection().getHost();
        try {
            camelContext.addRoutes(new RouteBuilder() {
                @Override public void configure() {
                    from(uri).routeId(routeId)
                        .split(body().tokenize("\n"))
                        .process(exchange -> {
                            String line = exchange.getIn().getBody(String.class);
                            if (line != null && !line.isBlank()) {
                                Map<String, Object> row = new LinkedHashMap<>();
                                row.put("line", line);
                                rows.add(row);
                            }
                        });
                }
            });
            Thread.sleep(10000);
            camelContext.getRouteController().stopRoute(routeId);
            camelContext.removeRoute(routeId);
        } catch (Exception e) { throw new RuntimeException("SFTP extraction failed", e); }
        log.info("SFTP extracted {} lines from {}", rows.size(), ds.getConnection().getHost());
        return rows.stream().map(Row::new);
    }
}
