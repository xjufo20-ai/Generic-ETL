package com.generic.etl.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generic.etl.extract.adapter.impl.CsvExtractor;
import com.generic.etl.extract.adapter.DataSourceManager;
import com.generic.etl.extract.adapter.ExtractorRegistry;
import com.generic.etl.extract.adapter.impl.JdbcExtractor;
import com.generic.etl.extract.adapter.WatermarkStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

@Configuration
public class ExtractConfig {

    @Bean public DataSourceManager dataSourceManager() { return new DataSourceManager(); }

    @Bean
    public WatermarkStore watermarkStore(ObjectMapper mapper) { return new WatermarkStore(Path.of("data"), mapper); }

    @Bean
    public JdbcExtractor jdbcExtractor(DataSourceManager dsManager, WatermarkStore watermarkStore) {
        return new JdbcExtractor(dsManager, watermarkStore);
    }

    @Bean public CsvExtractor csvExtractor() { return new CsvExtractor(); }

    @Bean
    public ExtractorRegistry extractorRegistry(JdbcExtractor jdbc, CsvExtractor csv) {
        return new ExtractorRegistry(List.of(jdbc, csv));
    }
}
