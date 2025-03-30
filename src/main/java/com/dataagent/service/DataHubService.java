package com.dataagent.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataHubService {

    private final DataHubClient dataHubClient;

    @Autowired
    public DataHubService(DataHubClient dataHubClient) {
        this.dataHubClient = dataHubClient;
    }

    public Map<String, String> getTableSchema(String projectId, String datasetId, String tableId) {
        Map<String, String> schema = new HashMap<>();
        try {
            Optional<Map<String, String>> schemaOpt = dataHubClient.getTableSchema(projectId, datasetId, tableId);
            if (schemaOpt.isPresent()) {
                schema.putAll(schemaOpt.get());
            }
        } catch (Exception e) {
            // Log error and return empty schema for now
            // In production, you might want to handle this differently
        }
        return schema;
    }
} 