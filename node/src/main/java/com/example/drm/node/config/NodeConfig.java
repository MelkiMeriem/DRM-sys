package com.example.drm.node.config;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "drm")
public record NodeConfig(
    String nodeId,
    int grpcPort,
    Map<String, String> peers
) {}

