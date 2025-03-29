package com.dataagent;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SqlLineageAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(SqlLineageAnalyzer.class);

    public void analyzeLineage(String sql) {
        try {
            // 获取表依赖关系和字段使用情况
            Map<String, Set<String>> tableColumns = extractTableAndColumns(sql);
            
            // 打印分析结果
            printAnalysis(tableColumns);
        } catch (Exception e) {
            log.error("Error analyzing SQL: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to analyze SQL", e);
        }
    }

    private Map<String, Set<String>> extractTableAndColumns(String sql) {
        Map<String, Set<String>> tableColumns = new HashMap<>();
        
        // 使用正则表达式提取表名和字段
        String fromPattern = "FROM\\s+`?([\\w.]+)`?\\s+(?:AS\\s+)?(\\w+)?";
        String joinPattern = "JOIN\\s+`?([\\w.]+)`?\\s+(?:AS\\s+)?(\\w+)?";
        String selectPattern = "SELECT\\s+(.*?)\\s+FROM";
        
        // 提取SELECT子句中的字段
        java.util.regex.Pattern selectRegex = java.util.regex.Pattern.compile(selectPattern, java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher selectMatcher = selectRegex.matcher(sql);
        if (selectMatcher.find()) {
            String selectClause = selectMatcher.group(1);
            // 提取字段名
            String columnPattern = "\\w+(?:\\.\\w+)?";
            java.util.regex.Pattern columnRegex = java.util.regex.Pattern.compile(columnPattern);
            java.util.regex.Matcher columnMatcher = columnRegex.matcher(selectClause);
            while (columnMatcher.find()) {
                String column = columnMatcher.group();
                String[] parts = column.split("\\.");
                if (parts.length > 1) {
                    String tableAlias = parts[0];
                    String columnName = parts[1];
                    // 将字段添加到对应表的集合中
                    tableColumns.computeIfAbsent(tableAlias, k -> new HashSet<>()).add(columnName);
                }
            }
        }
        
        // 提取FROM子句中的表
        java.util.regex.Pattern fromRegex = java.util.regex.Pattern.compile(fromPattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher fromMatcher = fromRegex.matcher(sql);
        if (fromMatcher.find()) {
            String tableName = fromMatcher.group(1);
            String alias = fromMatcher.group(2);
            if (alias != null) {
                // 如果表有别名，将之前收集的字段移动到正确的表名下
                Set<String> columns = tableColumns.remove(alias);
                if (columns != null) {
                    tableColumns.put(tableName, columns);
                }
            }
        }
        
        // 提取JOIN子句中的表
        java.util.regex.Pattern joinRegex = java.util.regex.Pattern.compile(joinPattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher joinMatcher = joinRegex.matcher(sql);
        while (joinMatcher.find()) {
            String tableName = joinMatcher.group(1);
            String alias = joinMatcher.group(2);
            if (alias != null) {
                // 如果表有别名，将之前收集的字段移动到正确的表名下
                Set<String> columns = tableColumns.remove(alias);
                if (columns != null) {
                    tableColumns.put(tableName, columns);
                }
            }
        }
        
        return tableColumns;
    }

    private void printAnalysis(Map<String, Set<String>> tableColumns) {
        System.out.println("\nSQL Analysis Results:");
        System.out.println("=====================");
        
        for (Map.Entry<String, Set<String>> entry : tableColumns.entrySet()) {
            System.out.println("\nSource Table: " + entry.getKey());
            System.out.println("Used Columns:");
            for (String column : entry.getValue()) {
                System.out.println("  - " + column);
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TableDependency {
        private String tableName;
        private Set<String> columns;
    }
} 