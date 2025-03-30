package com.dataagent.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

@Component
public class InMemoryDataHubClient implements DataHubClient {
    private final Map<String, Map<String, String>> schemaCache = new HashMap<>();

    public InMemoryDataHubClient() {
        // 初始化一些测试数据
        initializeTestData();
    }

    private void initializeTestData() {
        // 添加测试表的schema
        Map<String, String> userTableSchema = new HashMap<>();
        userTableSchema.put("id", "STRING");
        userTableSchema.put("name", "STRING");
        userTableSchema.put("email", "STRING");
        userTableSchema.put("created_at", "TIMESTAMP");
        schemaCache.put("test-project.test-dataset.users", userTableSchema);

        Map<String, String> orderTableSchema = new HashMap<>();
        orderTableSchema.put("order_id", "STRING");
        orderTableSchema.put("user_id", "STRING");
        orderTableSchema.put("amount", "FLOAT");
        orderTableSchema.put("status", "STRING");
        orderTableSchema.put("created_at", "TIMESTAMP");
        schemaCache.put("test-project.test-dataset.orders", orderTableSchema);

        // 添加更多测试数据
        Map<String, String> productTableSchema = new HashMap<>();
        productTableSchema.put("product_id", "STRING");
        productTableSchema.put("name", "STRING");
        productTableSchema.put("description", "STRING");
        productTableSchema.put("price", "DECIMAL");
        productTableSchema.put("category", "STRING");
        productTableSchema.put("in_stock", "BOOLEAN");
        schemaCache.put("test-project.test-dataset.products", productTableSchema);

        Map<String, String> orderItemTableSchema = new HashMap<>();
        orderItemTableSchema.put("order_item_id", "STRING");
        orderItemTableSchema.put("order_id", "STRING");
        orderItemTableSchema.put("product_id", "STRING");
        orderItemTableSchema.put("quantity", "INTEGER");
        orderItemTableSchema.put("unit_price", "DECIMAL");
        schemaCache.put("test-project.test-dataset.order_items", orderItemTableSchema);
    }

    @Override
    public Optional<Map<String, String>> getTableSchema(String projectId, String datasetId, String tableId) {
        String key = String.format("%s.%s.%s", projectId, datasetId, tableId);
        return Optional.ofNullable(schemaCache.get(key));
    }

    // 用于测试的辅助方法
    public void addTableSchema(String projectId, String datasetId, String tableId, Map<String, String> schema) {
        String key = String.format("%s.%s.%s", projectId, datasetId, tableId);
        schemaCache.put(key, schema);
    }

    public void clearCache() {
        schemaCache.clear();
        initializeTestData();
    }
} 