package com.generic.etl.api.config;

import com.generic.etl.extract.adapter.impl.CsvExtractor;
import com.generic.etl.extract.adapter.DataSourceManager;
import com.generic.etl.extract.adapter.ExtractorRegistry;
import com.generic.etl.extract.adapter.impl.JdbcExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class ExtractConfig {

    @Bean
    public DataSourceManager dataSourceManager() { return new DataSourceManager(); }

    @Bean
    public JdbcExtractor jdbcExtractor(DataSourceManager dsManager) { return new JdbcExtractor(dsManager); }

    @Bean
    public CsvExtractor csvExtractor() { return new CsvExtractor(); }

    @Bean
    public ExtractorRegistry extractorRegistry(JdbcExtractor jdbc, CsvExtractor csv) {
        return new ExtractorRegistry(List.of(jdbc, csv));
    }
}
