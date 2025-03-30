package com.dataagent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dataagent.service.DataHubService;

@Component
public class SqlLineageAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(SqlLineageAnalyzer.class);

    @Autowired
    private DataHubService dataHubService;

    public String analyzeLineage(String sql) {
        // 预处理SQL，移除注释和多余空白
        sql = preprocessSql(sql);
        
        // 存储表依赖和使用的列
        Map<String, Set<String>> tableColumns = new HashMap<>();
        Map<String, String> tableAliases = new HashMap<>();
        
        // 处理CTE
        sql = processCTE(sql, tableColumns, tableAliases);
        
        // 处理主查询
        processMainQuery(sql, tableColumns, tableAliases);
        
        // 处理子查询
        processSubqueries(sql, tableColumns, tableAliases);
        
        // 生成报告
        StringBuilder report = new StringBuilder();
        report.append("Table Dependencies:\n");
        for (String table : tableColumns.keySet()) {
            report.append("  - ").append(table).append("\n");
        }
        
        report.append("\nUsed Columns:\n");
        for (Map.Entry<String, Set<String>> entry : tableColumns.entrySet()) {
            report.append("  ").append(entry.getKey()).append(":\n");
            for (String column : entry.getValue()) {
                report.append("    - ").append(column).append("\n");
            }
        }

        // 添加schema信息
        report.append("\nSchema Information:\n");
        for (Map.Entry<String, Set<String>> entry : tableColumns.entrySet()) {
            String tableName = entry.getKey();
            String[] parts = tableName.split("\\.");
            if (parts.length == 3) {
                Map<String, String> schema = dataHubService.getTableSchema(parts[0], parts[1], parts[2]);
                if (!schema.isEmpty()) {
                    report.append("  ").append(tableName).append(":\n");
                    schema.forEach((field, type) -> 
                        report.append("    - ").append(field).append(" (").append(type).append(")\n"));
                }
            }
        }
        
        return report.toString();
    }

    private String preprocessSql(String sql) {
        // 移除SQL注释
        sql = sql.replaceAll("--.*$", ""); // 移除单行注释
        sql = sql.replaceAll("/\\*[\\s\\S]*?\\*/", ""); // 移除多行注释
        
        // 移除多余空白
        sql = sql.replaceAll("\\s+", " ").trim();
        
        return sql;
    }

    private String processCTE(String sql, Map<String, Set<String>> tableColumns, Map<String, String> tableAliases) {
        Pattern ctePattern = Pattern.compile("WITH\\s+([\\s\\S]+?)\\s+SELECT", Pattern.CASE_INSENSITIVE);
        Matcher cteMatcher = ctePattern.matcher(sql);
        
        if (cteMatcher.find()) {
            String ctePart = cteMatcher.group(1);
            String[] ctes = ctePart.split(",\\s*");
            
            for (String cte : ctes) {
                String[] parts = cte.trim().split("\\s+AS\\s*\\(", 2);
                if (parts.length == 2) {
                    String cteName = parts[0].trim();
                    String cteQuery = parts[1].substring(0, parts[1].lastIndexOf(")"));
                    processMainQuery(cteQuery, tableColumns, tableAliases);
                }
            }
        }
        
        return sql;
    }

    private void processMainQuery(String sql, Map<String, Set<String>> tableColumns, Map<String, String> tableAliases) {
        // 处理FROM和JOIN子句
        processTableReferences(sql, tableColumns, tableAliases);
        
        // 处理SELECT子句中的列引用
        processColumnReferences(sql, tableAliases, tableColumns);
    }

    private void processTableReferences(String sql, Map<String, Set<String>> tableColumns, Map<String, String> tableAliases) {
        // 匹配FROM和JOIN子句中的表引用
        Pattern tablePattern = Pattern.compile(
            "(?:FROM|JOIN)\\s+([\\w.`]+)(?:\\s+AS\\s+([\\w]+))?",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = tablePattern.matcher(sql);
        while (matcher.find()) {
            String tableName = matcher.group(1).replaceAll("`", "");
            String alias = matcher.group(2);
            
            if (alias != null) {
                tableAliases.put(alias, tableName);
            }
            
            tableColumns.computeIfAbsent(tableName, k -> new HashSet<>());
        }
    }

    private void processColumnReferences(String sql, Map<String, String> tableAliases, Map<String, Set<String>> tableColumns) {
        // 匹配SELECT子句中的列引用
        Pattern columnPattern = Pattern.compile(
            "(?:SELECT|WHERE|GROUP BY|ORDER BY|HAVING)\\s+([\\s\\S]+?)(?:FROM|$)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = columnPattern.matcher(sql);
        if (matcher.find()) {
            String columnsPart = matcher.group(1);
            
            // 匹配列引用
            Pattern columnRefPattern = Pattern.compile(
                "(?:([\\w.]+)\\.)?([\\w.]+)",
                Pattern.CASE_INSENSITIVE
            );
            
            Matcher columnRefMatcher = columnRefPattern.matcher(columnsPart);
            while (columnRefMatcher.find()) {
                String tableRef = columnRefMatcher.group(1);
                String columnName = columnRefMatcher.group(2);
                
                if (tableRef != null) {
                    // 处理带表引用的列
                    String tableName = tableAliases.getOrDefault(tableRef, tableRef);
                    if (tableColumns.containsKey(tableName)) {
                        tableColumns.get(tableName).add(columnName);
                    }
                } else {
                    // 处理不带表引用的列
                    for (String tableName : tableColumns.keySet()) {
                        tableColumns.get(tableName).add(columnName);
                    }
                }
            }
        }
    }

    private void processSubqueries(String sql, Map<String, Set<String>> tableColumns, Map<String, String> tableAliases) {
        // 匹配子查询
        Pattern subqueryPattern = Pattern.compile(
            "\\(\\s*SELECT\\s+[\\s\\S]+?\\s+FROM\\s+[\\s\\S]+?\\s+\\)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = subqueryPattern.matcher(sql);
        while (matcher.find()) {
            String subquery = matcher.group(0);
            processMainQuery(subquery.substring(1, subquery.length() - 1), tableColumns, tableAliases);
        }
    }
} 