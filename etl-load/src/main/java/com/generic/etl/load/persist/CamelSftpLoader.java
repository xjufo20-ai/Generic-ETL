package com.generic.etl.load.persist;

import com.generic.etl.common.model.PersistConfig;
import com.generic.etl.common.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;

import java.util.stream.Collectors;

/**
 * SFTP output via Apache Camel. Writes rows as CSV to a remote SFTP server.
 */
@Slf4j
public class CamelSftpLoader {
    private final CamelContext camelContext;

    public CamelSftpLoader(CamelContext camelContext) { this.camelContext = camelContext; }

    public void upload(java.util.List<Row> rows, PersistConfig.StorageConfig storage) {
        if (storage.getConnection() == null) return;
        String csv = rows.stream()
                .map(r -> r.getValues().values().stream()
                        .map(String::valueOf).collect(Collectors.joining(",")))
                .collect(Collectors.joining("\n"));

        String uri = String.format("sftp://%s@%s:%d%s?password=%s&fileName=output.csv",
                storage.getConnection().getUsername(), storage.getConnection().getUrl(),
                22, "/output", storage.getConnection().getPassword());

        ProducerTemplate template = camelContext.createProducerTemplate();
        template.sendBody(uri, csv);
        log.info("SFTP uploaded {} rows", rows.size());
    }
}
