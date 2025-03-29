package com.dataagent.controller;

import com.dataagent.service.SqlAnalyzerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SqlAnalyzerController {

    @Autowired
    private SqlAnalyzerService sqlAnalyzerService;

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/analyze")
    public String analyzeSql(@RequestParam String sql, Model model) {
        String result = sqlAnalyzerService.analyzeSql(sql);
        model.addAttribute("sql", sql);
        model.addAttribute("result", result);
        return "index";
    }
} 