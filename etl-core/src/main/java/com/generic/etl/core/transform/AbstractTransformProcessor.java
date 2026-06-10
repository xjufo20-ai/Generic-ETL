package com.generic.etl.core.transform;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.List;
import java.util.Map;

/**
 * Base class for Camel transform processors that operate on List&lt;Map&gt; bodies.
 * Subclasses only need to implement {@link #doTransform(List, Exchange)}.
 */
public abstract class AbstractTransformProcessor implements Processor {

    @Override
    @SuppressWarnings("unchecked")
    public final void process(Exchange exchange) {
        List<Map<String, Object>> rows = TransformUtils.bodyAsMapList(exchange);
        if (rows.isEmpty()) return;
        List<Map<String, Object>> result = doTransform(rows, exchange);
        if (result != null) {
            exchange.getIn().setBody(result);
        }
    }

    /**
     * Transform rows. Return null to leave the body unchanged.
     */
    protected abstract List<Map<String, Object>> doTransform(
            List<Map<String, Object>> rows, Exchange exchange);
}
