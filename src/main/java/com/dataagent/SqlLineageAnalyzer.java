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
        Set<String> leafTables = new HashSet<>();
        Set<String> intermediateTables = new HashSet<>();
        Set<String> usedColumns = new HashSet<>();
        
        try {
            // 处理WITH子句
            Pattern withPattern = Pattern.compile("WITH\\s+([^\\s]+)\\s+AS\\s*\\(([^()]+|\\([^()]*\\))*\\)(?:\\s*,\\s*([^\\s]+)\\s+AS\\s*\\(([^()]+|\\([^()]*\\))*\\))*\\s*SELECT", 
                Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
            Matcher withMatcher = withPattern.matcher(sql);
            
            while (withMatcher.find()) {
                // 处理第一个CTE
                String cteName = withMatcher.group(1).trim();
                String cteQuery = withMatcher.group(2).trim();
                intermediateTables.add(cteName);
                processQuery(cteQuery, tableColumns, tableAliases, leafTables, intermediateTables, usedColumns);
                
                // 处理后续的CTE
                if (withMatcher.group(3) != null) {
                    String secondCteName = withMatcher.group(3).trim();
                    String secondCteQuery = withMatcher.group(4).trim();
                    intermediateTables.add(secondCteName);
                    processQuery(secondCteQuery, tableColumns, tableAliases, leafTables, intermediateTables, usedColumns);
                }
            }
            
            // 移除WITH子句，处理主查询
            String mainQuery = sql.replaceAll("WITH\\s+[^\\s]+\\s+AS\\s*\\((?:[^()]+|\\([^()]*\\))*\\)(?:\\s*,\\s*[^\\s]+\\s+AS\\s*\\((?:[^()]+|\\([^()]*\\))*\\))*\\s*", "");
            processQuery(mainQuery, tableColumns, tableAliases, leafTables, intermediateTables, usedColumns);
            
            // 从leafTables中移除中间表
            leafTables.removeAll(intermediateTables);
            
            // 从tableColumns中移除中间表的列
            intermediateTables.forEach(tableColumns::remove);
            
        } catch (Exception e) {
            log.error("Failed to parse SQL: {}", sql, e);
            return "Error: Failed to parse SQL";
        }
        
        // 生成报告，只包含叶子节点表
        StringBuilder report = new StringBuilder();
        report.append("Table Dependencies:\n");
        for (String table : leafTables) {
            report.append("  - ").append(table).append("\n");
        }
        
        report.append("\nUsed Columns:\n");
        for (String table : leafTables) {
            Set<String> columns = tableColumns.get(table);
            if (columns != null) {
                report.append("  ").append(table).append(":\n");
                for (String column : columns) {
                    if (usedColumns.contains(column)) {
                        report.append("    - ").append(column).append("\n");
                    }
                }
            }
        }

        // 添加schema信息，只包含叶子节点表
        report.append("\nSchema Information:\n");
        for (String table : leafTables) {
            String[] parts = table.split("\\.");
            if (parts.length == 3) {
                Map<String, String> schema = dataHubService.getTableSchema(parts[0], parts[1], parts[2]);
                if (!schema.isEmpty()) {
                    report.append("  ").append(table).append(":\n");
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
        
        // 处理反引号
        sql = sql.replaceAll("`([^`]+)`", "$1");
        
        return sql;
    }

    private void processQuery(String sql, Map<String, Set<String>> tableColumns,
                            Map<String, String> tableAliases, Set<String> leafTables,
                            Set<String> intermediateTables, Set<String> usedColumns) {
        // 处理FROM子句
        Pattern fromPattern = Pattern.compile("FROM\\s+([^\\s]+)(?:\\s+(?:AS\\s+)?([^\\s]+))?", Pattern.CASE_INSENSITIVE);
        Matcher fromMatcher = fromPattern.matcher(sql);
        
        while (fromMatcher.find()) {
            String tableName = fromMatcher.group(1).trim();
            String alias = fromMatcher.group(2) != null ? fromMatcher.group(2).trim() : null;
            
            if (alias != null) {
                tableAliases.put(alias, tableName);
            }
            
            if (!intermediateTables.contains(tableName)) {
                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>());
                leafTables.add(tableName);
            }
        }
        
        // 处理JOIN子句
        Pattern joinPattern = Pattern.compile("JOIN\\s+([^\\s]+)(?:\\s+(?:AS\\s+)?([^\\s]+))?", Pattern.CASE_INSENSITIVE);
        Matcher joinMatcher = joinPattern.matcher(sql);
        
        while (joinMatcher.find()) {
            String tableName = joinMatcher.group(1).trim();
            String alias = joinMatcher.group(2) != null ? joinMatcher.group(2).trim() : null;
            
            if (alias != null) {
                tableAliases.put(alias, tableName);
            }
            
            if (!intermediateTables.contains(tableName)) {
                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>());
                leafTables.add(tableName);
            }
        }
        
        // 处理SELECT子句
        Pattern selectPattern = Pattern.compile("SELECT\\s+(.+?)\\s+FROM", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher selectMatcher = selectPattern.matcher(sql);
        
        if (selectMatcher.find()) {
            String selectClause = selectMatcher.group(1).trim();
            String[] columns = selectClause.split(",");
            
            for (String column : columns) {
                column = column.trim();
                if (column.contains(".")) {
                    String[] parts = column.split("\\.");
                    String tableRef = parts[0].trim();
                    String columnName = parts[1].trim();
                    
                    String tableName = tableAliases.getOrDefault(tableRef, tableRef);
                    if (tableColumns.containsKey(tableName) && !intermediateTables.contains(tableName)) {
                        tableColumns.get(tableName).add(columnName);
                        usedColumns.add(columnName);
                    }
                } else {
                    for (String tableName : tableColumns.keySet()) {
                        if (!intermediateTables.contains(tableName)) {
                            tableColumns.get(tableName).add(column);
                            usedColumns.add(column);
                        }
                    }
                }
            }
        }
    }
} 