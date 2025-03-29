package com.dataagent.service;

import org.springframework.stereotype.Service;

import com.dataagent.SqlLineageAnalyzer;

@Service
public class SqlAnalyzerService {

    private final SqlLineageAnalyzer analyzer;

    public SqlAnalyzerService() {
        this.analyzer = new SqlLineageAnalyzer();
    }

    public String analyzeSql(String sql) {
        try {
            // 直接返回分析结果
            return analyzer.analyzeLineage(sql);
        } catch (Exception e) {
            StringBuilder errorResult = new StringBuilder();
            errorResult.append("Error analyzing SQL:\n");
            errorResult.append("==================\n");
            errorResult.append(e.getMessage());
            return errorResult.toString();
        }
    }
} 