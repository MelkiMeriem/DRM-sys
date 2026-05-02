package com.example.drm.node.grpc;

import com.example.drm.node.config.NodeConfig;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class GrpcServerLifecycle implements SmartLifecycle {
  private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

  private final NodeConfig config;
  private final JobServiceImpl jobService;
  private final ClusterServiceImpl clusterService;

  private volatile boolean running = false;
  private Server server;

  public GrpcServerLifecycle(NodeConfig config, JobServiceImpl jobService, ClusterServiceImpl clusterService) {
    this.config = config;
    this.jobService = jobService;
    this.clusterService = clusterService;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    server = NettyServerBuilder.forPort(config.grpcPort())
        .addService(jobService)
        .addService(clusterService)
        .build();
    try {
      server.start();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start gRPC server", e);
    }
    running = true;
    log.info("gRPC server started on port {} (nodeId={})", config.grpcPort(), config.nodeId());
  }

  @Override
  public void stop() {
    if (!running) {
      return;
    }
    server.shutdown();
    try {
      if (!server.awaitTermination(3, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      server.shutdownNow();
    }
    running = false;
    log.info("gRPC server stopped (nodeId={})", config.nodeId());
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}

