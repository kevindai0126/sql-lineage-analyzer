package com.dataagent;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MockDataHubClient {
    private static final Logger log = LoggerFactory.getLogger(MockDataHubClient.class);
    private final Map<String, TableMetadata> tables = new HashMap<>();
    private final Map<String, Set<String>> lineage = new HashMap<>();

    public MockDataHubClient() {
        initializeTestTables();
    }

    private void initializeTestTables() {
        // 初始化测试表 (使用BigQuery格式的表名)
        tables.put("project.dataset.users", new TableMetadata("project.dataset.users", Arrays.asList("id", "name", "age", "status")));
        tables.put("project.dataset.orders", new TableMetadata("project.dataset.orders", Arrays.asList("id", "user_id", "product_id", "amount", "status")));
        tables.put("project.dataset.products", new TableMetadata("project.dataset.products", Arrays.asList("id", "name", "price", "category")));

        // 初始化血缘关系
        lineage.put("project.dataset.orders", new HashSet<>(Arrays.asList("project.dataset.users")));
        lineage.put("project.dataset.products", new HashSet<>(Arrays.asList("project.dataset.orders")));
    }

    public TableMetadata getTableMetadata(String tableName) {
        return tables.get(tableName);
    }

    public void createLineage(String tableName, String dependentTable, String relationshipType) {
        lineage.computeIfAbsent(tableName, k -> new HashSet<>()).add(dependentTable);
        log.info("Created lineage relationship: {} -> {} ({})", tableName, dependentTable, relationshipType);
    }

    public void printLineage() {
        System.out.println("\nCurrent Lineage in Mock DataHub:");
        System.out.println("================================");
        for (Map.Entry<String, Set<String>> entry : lineage.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            for (String dependent : entry.getValue()) {
                System.out.println("  Depends on: " + dependent);
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TableMetadata {
        private String tableName;
        private List<String> columns;
    }
} 