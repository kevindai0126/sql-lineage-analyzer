package com.dataagent.service;

import java.util.Map;
import java.util.Optional;

public interface DataHubClient {
    Optional<Map<String, String>> getTableSchema(String projectId, String datasetId, String tableId);
} 