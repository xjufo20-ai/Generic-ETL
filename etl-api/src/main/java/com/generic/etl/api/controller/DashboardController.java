package com.generic.etl.api.controller;

import com.generic.etl.core.store.AuditLog;
import com.generic.etl.core.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/dashboard")
@RequiredArgsConstructor
public class DashboardController {
    private final StateStore store;
    private final LineageStore lineageStore;
    private final AuditLog auditLog;

    @GetMapping
    public String index(Model model) {
        var pipelines = store.getAllPipelines().keySet().stream().sorted().toList();
        var recentAudit = auditLog.getHistory().stream().limit(20).toList();

        model.addAttribute("pipelines", pipelines);
        model.addAttribute("lineage", lineageStore.getAll());
        model.addAttribute("pipelineCount", pipelines.size());
        model.addAttribute("runCount", auditLog.getHistory().size());
        model.addAttribute("recentAudit", recentAudit);
        return "dashboard";
    }
}
