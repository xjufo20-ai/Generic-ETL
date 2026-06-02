package com.generic.etl.core.transform;

import com.generic.etl.common.model.Row;
import com.generic.etl.common.model.TransformDef;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Bridges TransformProcessor chain to Camel Processor.
 * Converts Exchange body (List<Map>) → List<Row> → transform → List<Map>.
 */
public class CamelTransformAdapter implements Processor {

    private final TransformChain transformChain;
    private final List<TransformDef> transformDefs;

    public CamelTransformAdapter(Map<String, TransformProcessor> processors,
                                  List<TransformDef> transformDefs) {
        this.transformChain = new TransformChain(processors);
        this.transformDefs = transformDefs;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> body = exchange.getIn().getBody(List.class);
        if (body == null || body.isEmpty()) {
            exchange.getIn().setBody(List.of());
            return;
        }

        // Map → Row → transform → Map
        List<Row> rows = body.stream()
                .map(m -> new Row(new LinkedHashMap<>(m)))
                .collect(Collectors.toList());

        List<Row> transformed = transformChain.apply(rows.stream(), transformDefs).toList();

        List<Map<String, Object>> result = transformed.stream()
                .map(Row::getValues)
                .collect(Collectors.toList());

        exchange.getIn().setBody(result);
    }

    /** Convenience factory. */
    public static Processor of(Map<String, TransformProcessor> processors,
                                List<TransformDef> defs) {
        return new CamelTransformAdapter(processors, defs);
    }
}
