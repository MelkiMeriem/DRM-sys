package com.example.drm.node.raft;

import com.example.drm.proto.ClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class PeerClients implements AutoCloseable {
  private final Map<String, ManagedChannel> channelsByNodeId = new ConcurrentHashMap<>();

  public ClusterServiceGrpc.ClusterServiceBlockingStub blockingStub(Peer peer) {
    ManagedChannel channel = channelsByNodeId.computeIfAbsent(peer.nodeId(), id ->
        ManagedChannelBuilder.forTarget("dns:///" + peer.target()).usePlaintext().build()
    );
    return ClusterServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public void close() {
    for (ManagedChannel channel : channelsByNodeId.values()) {
      channel.shutdownNow();
    }
  }
}

