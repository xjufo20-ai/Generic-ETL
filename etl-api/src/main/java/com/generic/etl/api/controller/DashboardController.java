package com.generic.etl.api.controller;

import com.generic.etl.api.config.CamelRouteFactory;
import com.generic.etl.api.store.AuditLog;
import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/dashboard")
public class DashboardController {
    private final StateStore store;
    private final LineageStore lineageStore;
    private final CamelRouteFactory routeFactory;
    private final AuditLog auditLog;

    public DashboardController(StateStore store, LineageStore lineageStore,
                                CamelRouteFactory routeFactory, AuditLog auditLog) {
        this.store = store;
        this.lineageStore = lineageStore;
        this.routeFactory = routeFactory;
        this.auditLog = auditLog;
    }

    @GetMapping
    public String index(Model model) {
        var pipelines = store.getAllPipelines().keySet().stream().sorted().toList();
        model.addAttribute("pipelines", pipelines);
        model.addAttribute("routes", routeFactory.getRegisteredPipelines());
        model.addAttribute("lineage", lineageStore.getAll());
        model.addAttribute("pipelineCount", pipelines.size());
        model.addAttribute("routeCount", routeFactory.getRegisteredPipelines().size());
        model.addAttribute("recentAudit", auditLog.getHistory().stream().limit(20).toList());
        return "dashboard";
    }
}
