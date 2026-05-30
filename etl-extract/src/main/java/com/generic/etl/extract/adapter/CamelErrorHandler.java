package com.generic.etl.extract.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Global Camel error handling: Dead Letter Channel with redelivery.
 * Failed messages are logged with full context; after max redeliveries they are dropped.
 * A Dead Letter Queue (Kafka/JMS/DB) can be plugged in here for production.
 */
@Slf4j
@Component
@ConditionalOnProperty(value = "camel.error-handler.enabled", havingValue = "true", matchIfMissing = true)
public class CamelErrorHandler extends RouteBuilder {

    @Override
    public void configure() {
        // Global Dead Letter Channel: retry up to 3 times with exponential backoff
        errorHandler(deadLetterChannel("log:dead?level=ERROR&showAll=true&multiline=true")
                .maximumRedeliveries(3)
                .redeliveryDelay(1000)
                .backOffMultiplier(2)
                .retryAttemptedLogLevel(org.apache.camel.LoggingLevel.WARN));

        // Handle JDBC-specific errors
        onException(java.sql.SQLException.class)
                .maximumRedeliveries(2)
                .redeliveryDelay(2000)
                .backOffMultiplier(1.5)
                .logExhausted(true)
                .handled(true)
                .to("log:jdbc-error?level=ERROR&showAll=true");

        // Handle generic extraction errors
        onException(RuntimeException.class)
                .maximumRedeliveries(3)
                .redeliveryDelay(1000)
                .backOffMultiplier(2)
                .logExhausted(true)
                .handled(true)
                .to("log:extract-error?level=ERROR&showAll=true");
    }
}
