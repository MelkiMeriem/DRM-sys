package com.example.drm.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class NodeApplication {
  public static void main(String[] args) {
    SpringApplication.run(NodeApplication.class, args);
  }
}

