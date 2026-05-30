package com.generic.etl.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI genericEtlOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Generic-ETL API")
                        .version("0.1.0")
                        .description("JSON-configurable ETL engine — pipeline management, execution, consumer registration")
                        .contact(new Contact().name("Generic-ETL Team")));
    }
}
