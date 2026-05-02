package com.example.drm.node.web;

import com.example.drm.node.config.NodeConfig;
import com.example.drm.node.raft.Peer;
import com.example.drm.node.raft.PeerClients;
import com.example.drm.node.raft.RaftNode;
import com.example.drm.proto.GetNodeStatusRequest;
import com.example.drm.proto.GetNodeStatusResponse;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DashboardController {
  private final NodeConfig config;
  private final RaftNode raft;
  private final PeerClients peerClients;

  public DashboardController(NodeConfig config, RaftNode raft, PeerClients peerClients) {
    this.config = config;
    this.raft = raft;
    this.peerClients = peerClients;
  }

  @GetMapping("/")
  public String index(Model model) {
    model.addAttribute("nodeId", config.nodeId());
    model.addAttribute("grpcPort", config.grpcPort());
    model.addAttribute("role", raft.role().name());
    model.addAttribute("term", raft.term());
    model.addAttribute("leaderId", raft.leaderId());
    model.addAttribute("lastHeartbeat", raft.lastHeartbeatEpochMs());

    List<GetNodeStatusResponse> peerStatuses = new ArrayList<>();
    for (Peer peer : raft.peers()) {
      try {
        peerStatuses.add(peerClients.blockingStub(peer).getNodeStatus(GetNodeStatusRequest.newBuilder().build()));
      } catch (StatusRuntimeException e) {
        peerStatuses.add(GetNodeStatusResponse.newBuilder()
            .setNodeId(peer.nodeId())
            .setRole("UNREACHABLE")
            .setLeaderId("")
            .setTerm(0)
            .build());
      }
    }
    model.addAttribute("peers", peerStatuses);
    return "index";
  }
}

