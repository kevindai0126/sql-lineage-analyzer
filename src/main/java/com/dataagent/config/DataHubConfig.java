package com.dataagent.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.dataagent.service.DataHubClient;
import com.dataagent.service.InMemoryDataHubClient;

@Configuration
@Profile("!test")
public class DataHubConfig {

    @Value("${datahub.environment:local}")
    private String environment;

    @Bean
    public DataHubClient dataHubClient() {
        return new InMemoryDataHubClient();
    }
} 