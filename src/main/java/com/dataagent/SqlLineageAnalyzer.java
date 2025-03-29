package com.dataagent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlLineageAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(SqlLineageAnalyzer.class);

    public String analyzeLineage(String sql) {
        try {
            // 预处理SQL，移除注释和多余空白
            String processedSql = preprocessSql(sql);
            
            // 分析表依赖关系
            Map<String, Set<String>> tableColumns = new HashMap<>();
            
            // 处理WITH子句（CTE）
            if (processedSql.toUpperCase().startsWith("WITH")) {
                processedSql = processCTE(processedSql, tableColumns);
            }
            
            // 处理主查询
            processMainQuery(processedSql, tableColumns);
            
            // 返回分析结果
            return formatAnalysis(tableColumns);
        } catch (Exception e) {
            log.error("Error analyzing SQL: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to analyze SQL: " + e.getMessage(), e);
        }
    }

    private String preprocessSql(String sql) {
        // 移除SQL注释
        sql = sql.replaceAll("--.*$", "") // 移除单行注释
                 .replaceAll("/\\*[\\s\\S]*?\\*/", "") // 移除多行注释
                 .replaceAll("\\s+", " ") // 将多个空白字符替换为单个空格
                 .trim();
        return sql;
    }

    private String processCTE(String sql, Map<String, Set<String>> tableColumns) {
        // 提取WITH子句中的所有CTE
        String ctePattern = "WITH\\s+(?:RECURSIVE\\s+)?(.*?)\\s+SELECT";
        Pattern pattern = Pattern.compile(ctePattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            String cteDefinitions = matcher.group(1);
            // 处理每个CTE定义
            String[] ctes = cteDefinitions.split("(?i)\\s*,\\s*(?=\\w+\\s+AS\\s*\\(\\s*SELECT)");
            for (String cte : ctes) {
                processCTEDefinition(cte, tableColumns);
            }
            // 返回主查询部分
            return sql.substring(matcher.end());
        }
        return sql;
    }

    private void processCTEDefinition(String cte, Map<String, Set<String>> tableColumns) {
        // 提取CTE名称和查询
        String ctePattern = "(\\w+)\\s+AS\\s*\\((.*?)\\)";
        Pattern pattern = Pattern.compile(ctePattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(cte);
        
        if (matcher.find()) {
            String cteName = matcher.group(1);
            String cteQuery = matcher.group(2);
            // 处理CTE查询
            processMainQuery(cteQuery, tableColumns);
        }
    }

    private void processMainQuery(String sql, Map<String, Set<String>> tableColumns) {
        // 处理子查询
        sql = processSubqueries(sql, tableColumns);
        
        // 处理表引用
        processTableReferences(sql, tableColumns);
        
        // 处理字段引用
        processColumnReferences(sql, tableColumns);
    }

    private String processSubqueries(String sql, Map<String, Set<String>> tableColumns) {
        // 提取子查询
        String subqueryPattern = "\\(\\s*SELECT\\s+.*?\\)";
        Pattern pattern = Pattern.compile(subqueryPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        while (matcher.find()) {
            String subquery = matcher.group();
            // 递归处理子查询
            processMainQuery(subquery.substring(1, subquery.length() - 1), tableColumns);
        }
        
        return sql;
    }

    private void processTableReferences(String sql, Map<String, Set<String>> tableColumns) {
        // 处理FROM子句
        String fromPattern = "FROM\\s+`?([\\w.]+)`?\\s+(?:AS\\s+)?(\\w+)?";
        processTablePattern(sql, fromPattern, tableColumns);
        
        // 处理JOIN子句
        String joinPattern = "JOIN\\s+`?([\\w.]+)`?\\s+(?:AS\\s+)?(\\w+)?";
        processTablePattern(sql, joinPattern, tableColumns);
        
        // 处理UNION/UNION ALL
        String unionPattern = "UNION\\s+(?:ALL\\s+)?SELECT\\s+.*?FROM\\s+`?([\\w.]+)`?";
        processTablePattern(sql, unionPattern, tableColumns);
    }

    private void processTablePattern(String sql, String pattern, Map<String, Set<String>> tableColumns) {
        Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = regex.matcher(sql);
        
        while (matcher.find()) {
            String tableName = matcher.group(1);
            String alias = matcher.groupCount() > 1 ? matcher.group(2) : null;
            if (alias != null) {
                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>());
            }
        }
    }

    private void processColumnReferences(String sql, Map<String, Set<String>> tableColumns) {
        // 提取SELECT子句中的字段
        String selectPattern = "SELECT\\s+(.*?)\\s+FROM";
        Pattern pattern = Pattern.compile(selectPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            String selectClause = matcher.group(1);
            // 提取字段名
            String columnPattern = "\\w+(?:\\.\\w+)?";
            Pattern columnRegex = Pattern.compile(columnPattern);
            Matcher columnMatcher = columnRegex.matcher(selectClause);
            
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
    }

    private String formatAnalysis(Map<String, Set<String>> tableColumns) {
        StringBuilder result = new StringBuilder();
        
        if (tableColumns.isEmpty()) {
            result.append("No table dependencies found in the SQL query.\n");
            return result.toString();
        }

        result.append("Table Dependencies:\n");
        result.append("==================\n\n");
        
        for (Map.Entry<String, Set<String>> entry : tableColumns.entrySet()) {
            result.append("Source Table: ").append(entry.getKey()).append("\n");
            Set<String> columns = entry.getValue();
            if (!columns.isEmpty()) {
                result.append("Used Columns:\n");
                for (String column : columns) {
                    result.append("  - ").append(column).append("\n");
                }
            } else {
                result.append("All columns used\n");
            }
            result.append("\n");
        }
        
        return result.toString();
    }
} 