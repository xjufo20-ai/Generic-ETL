package com.generic.etl.api.controller;

import com.generic.etl.api.store.LineageStore;
import com.generic.etl.api.store.StateStore;
import com.generic.etl.common.model.PipelineRun;
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

    }

    @GetMapping
    public String index(Model model) {
        model.addAttribute("pipelines", store.getAllPipelines().keySet().stream().sorted().toList());
        model.addAttribute("recentRuns", runs.size() > 10 ? runs.subList(runs.size() - 10, runs.size()) : runs);
        model.addAttribute("lineage", lineageStore.getAll());
        model.addAttribute("pipelineCount", store.getAllPipelines().size());
        model.addAttribute("runCount", runs.size());
        return "dashboard";
    }
}

