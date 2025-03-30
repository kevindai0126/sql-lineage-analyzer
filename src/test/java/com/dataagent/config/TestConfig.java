package com.dataagent.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import com.dataagent.service.DataHubClient;
import com.dataagent.service.InMemoryDataHubClient;

@Configuration
@Profile("test")
public class TestConfig {
    
    @Bean
    @Primary
    public DataHubClient dataHubClient() {
        return new InMemoryDataHubClient();
    }
} 