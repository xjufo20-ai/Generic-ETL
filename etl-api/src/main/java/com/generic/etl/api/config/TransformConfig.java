package com.generic.etl.api.config;

import com.generic.etl.core.transform.TransformPipeline;
import com.generic.etl.core.transform.TransformProcessor;
import com.generic.etl.transform.impl.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class TransformConfig {

    @Bean public FilterProcessor filterProcessor() { return new FilterProcessor(); }
    @Bean public RenameProcessor renameProcessor() { return new RenameProcessor(); }
    @Bean public TypeCastProcessor typeCastProcessor() { return new TypeCastProcessor(); }
    @Bean public AggregateProcessor aggregateProcessor() { return new AggregateProcessor(); }
    @Bean public JoinProcessor joinProcessor(DataSource dataSource) { return new JoinProcessor(dataSource); }
    @Bean public SplitProcessor splitProcessor() { return new SplitProcessor(); }

    @Bean
    public TransformPipeline transformPipeline(List<TransformProcessor> processors) {
        Map<String, TransformProcessor> map = new LinkedHashMap<>();
        map.put("filter", find(processors, FilterProcessor.class));
        map.put("rename", find(processors, RenameProcessor.class));
        map.put("typeCast", find(processors, TypeCastProcessor.class));
        map.put("aggregate", find(processors, AggregateProcessor.class));
        map.put("join", find(processors, JoinProcessor.class));
        map.put("split", find(processors, SplitProcessor.class));
        return new TransformPipeline(map);
    }

    private TransformProcessor find(List<TransformProcessor> list, Class<?> type) {
        return list.stream().filter(p -> type.isAssignableFrom(p.getClass())).findFirst()
                .orElseThrow(() -> new IllegalStateException("Missing processor: " + type.getSimpleName()));
    }
}
