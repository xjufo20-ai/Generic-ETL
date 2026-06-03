package com.generic.etl.api.controller;

import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import org.apache.camel.CamelContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/dashboard")
public class DashboardController {
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;
    private final CamelContext camelContext;

    public DashboardController(StateStore store, LineageStore lineageStore,
                                AuditLog auditLog, CamelContext camelContext) {
        this.store = store;
        this.lineageStore = lineageStore;
        this.auditLog = auditLog;
        this.camelContext = camelContext;
    }

    @GetMapping
    public String index(Model model) {
        var pipelines = store.getAllPipelines().keySet().stream().sorted().toList();
        model.addAttribute("pipelines", pipelines);
        model.addAttribute("routes", camelContext.getRoutes().stream().map(r -> r.getRouteId()).toList());
        model.addAttribute("lineage", lineageStore.getAll());
        model.addAttribute("pipelineCount", pipelines.size());
        model.addAttribute("routeCount", camelContext.getRoutes().size());
        model.addAttribute("recentAudit", auditLog.getHistory().stream().limit(20).toList());
        return "dashboard";
    }
}
