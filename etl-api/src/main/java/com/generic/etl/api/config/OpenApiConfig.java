package com.generic.etl.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Value("${info.app.version:0.1.0}")
    private String version;

    @Bean
    public OpenAPI genericEtlOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Generic-ETL API")
                        .version(version)
                        .description("JSON-configurable ETL engine — pipeline management, execution, consumer registration")
                        .contact(new Contact().name("Generic-ETL Team")));
    }
}
