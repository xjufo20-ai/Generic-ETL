package com.generic.etl.api.config;

import com.generic.etl.common.model.*;
import org.apache.camel.CamelContext;

/** Builds Camel endpoint URIs from PipelineConfig datasource. */
public class SourceUriBuilder {

    private final CamelContext camelContext;

    public SourceUriBuilder(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public String build(PipelineConfig config) {
        DataSourceConfig ds = config.getDatasource();

        if (ds instanceof DataSourceConfig.JdbcDataSource j) {
            String q = applyWatermark(j.getQuery(), config);
            String ref = "sql-" + config.getPipeline().getName();
            camelContext.getRegistry().bind(ref, q);
            return "jdbc:etlDataSource?outputType=StreamList&query=#" + ref;
        }
        if (ds instanceof DataSourceConfig.KafkaDataSource k) {
            return String.format("kafka:%s?brokers=%s&groupId=%s",
                    k.getConnection().getTopic(),
                    k.getConnection().getBootstrapServers(),
                    k.getConnection().getGroupId());
        }
        if (ds instanceof DataSourceConfig.CsvDataSource c) {
            return "file:" + c.getFilePath() + "?noop=true&charset=UTF-8";
        }
        if (ds instanceof DataSourceConfig.SftpDataSource s) {
            return String.format("sftp://%s@%s:%d%s?password=RAW(%s)&fileName=%s",
                    s.getConnection().getUsername(), s.getConnection().getHost(),
                    s.getConnection().getPort() > 0 ? s.getConnection().getPort() : 22,
                    s.getConnection().getDirectory(), s.getConnection().getPassword(),
                    s.getFileName() != null ? s.getFileName() : "*.*");
        }
        throw new IllegalArgumentException("Unsupported datasource: " + ds.getType());
    }

    private String applyWatermark(String q, PipelineConfig c) {
        if (c.getWatermark() == null || c.getWatermark().getInitial() == null) return q;
        return q + " AND " + c.getWatermark().getColumn() + " >= '" + c.getWatermark().getInitial() + "'";
    }
}
