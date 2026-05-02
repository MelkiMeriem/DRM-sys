package com.example.drm.client;

import com.example.drm.proto.JobServiceGrpc;
import com.example.drm.proto.JobType;
import com.example.drm.proto.SubmitJobRequest;
import com.example.drm.proto.SubmitJobResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class DrmClient {
  // Sujet: le client a une liste de nœuds et en choisit UN seul aléatoirement.
  private static final List<String> DEFAULT_NODES = List.of("localhost:50051", "localhost:50052", "localhost:50053");

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("""
Usage:
  java -jar client/target/client-1.0.0-SNAPSHOT.jar sum   '{\"numbers\":[1,2,3]}'
  java -jar client/target/client-1.0.0-SNAPSHOT.jar sha256 '{\"text\":\"hello\"}'
  java -jar client/target/client-1.0.0-SNAPSHOT.jar msg   '{\"message\":\"bonjour\"}'
Optional env:
  DRM_NODES=host1:50051,host2:50052,host3:50053
""");
      System.exit(2);
    }

    JobType type = parseType(args[0]);
    String payloadJson = args[1];

    List<String> nodes = parseNodes(System.getenv("DRM_NODES"));
    String target = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

    ManagedChannel channel = NettyChannelBuilder.forTarget("dns:///" + target).usePlaintext().build();
    try {
      JobServiceGrpc.JobServiceBlockingStub stub = JobServiceGrpc.newBlockingStub(channel);
      SubmitJobResponse resp = stub.submitJob(
          SubmitJobRequest.newBuilder()
              .setType(type)
              .setPayloadJson(payloadJson)
              .setClientRequestId(String.valueOf(System.nanoTime()))
              .build()
      );
      System.out.println("Contacted: " + target);
      System.out.println("JobId:     " + resp.getJob().getJobId());
      System.out.println("Status:    " + resp.getJob().getStatus());
      if (!resp.getLeaderNodeId().isEmpty()) {
        System.out.println("Leader:    " + resp.getLeaderNodeId());
      }
      if (!resp.getJob().getResultJson().isEmpty()) {
        System.out.println("Result:    " + resp.getJob().getResultJson());
      }
    } finally {
      channel.shutdownNow();
    }
  }

  private static JobType parseType(String t) {
    return switch (t.toLowerCase()) {
      case "sum" -> JobType.JOB_TYPE_SUM;
      case "sha256", "hash" -> JobType.JOB_TYPE_SHA256;
      case "msg", "message" -> JobType.JOB_TYPE_MESSAGE;
      default -> throw new IllegalArgumentException("Unknown job type: " + t + " (expected: sum|sha256|msg)");
    };
  }

  private static List<String> parseNodes(String env) {
    if (env == null || env.isBlank()) {
      return DEFAULT_NODES;
    }
    return List.of(env.split(","));
  }

  // keep parsing helpers available later if we add flags
}

