package com.example.drm.node.grpc;

import com.example.drm.node.config.NodeConfig;
import com.example.drm.node.jobs.JobManager;
import com.example.drm.node.raft.Peer;
import com.example.drm.node.raft.PeerClients;
import com.example.drm.node.raft.RaftNode;
import com.example.drm.proto.ClusterServiceGrpc;
import com.example.drm.proto.ForwardJobRequest;
import com.example.drm.proto.ForwardJobResponse;
import com.example.drm.proto.GetJobRequest;
import com.example.drm.proto.GetJobResponse;
import com.example.drm.proto.Job;
import com.example.drm.proto.JobServiceGrpc;
import com.example.drm.proto.SubmitJobRequest;
import com.example.drm.proto.SubmitJobResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
  private final NodeConfig config;
  private final RaftNode raft;
  private final PeerClients peerClients;
  private final JobManager jobManager;

  public JobServiceImpl(NodeConfig config, RaftNode raft, PeerClients peerClients, JobManager jobManager) {
    this.config = config;
    this.raft = raft;
    this.peerClients = peerClients;
    this.jobManager = jobManager;
  }

  @Override
  public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
    if (!raft.isLeader()) {
      ForwardJobResponse forwarded = forwardToLeader(request);
      responseObserver.onNext(forwarded.getSubmit().toBuilder().setLeaderNodeId(raft.leaderId()).build());
      responseObserver.onCompleted();
      return;
    }

    Job job = jobManager.submit(request.getType(), request.getPayloadJson(), config.nodeId());
    responseObserver.onNext(SubmitJobResponse.newBuilder().setJob(job).setLeaderNodeId(config.nodeId()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    Job job = jobManager.get(request.getJobId());
    if (job == null) {
      responseObserver.onError(Status.NOT_FOUND.withDescription("Unknown job_id").asRuntimeException());
      return;
    }
    responseObserver.onNext(GetJobResponse.newBuilder().setJob(job).setLeaderNodeId(Optional.ofNullable(raft.leaderId()).orElse("")).build());
    responseObserver.onCompleted();
  }

  private ForwardJobResponse forwardToLeader(SubmitJobRequest request) {
    String leaderId = raft.leaderId();
    if (leaderId == null) {
      throw Status.UNAVAILABLE.withDescription("No leader elected yet").asRuntimeException();
    }

    Peer leader = raft.peers().stream()
        .filter(p -> p.nodeId().equals(leaderId))
        .findFirst()
        .orElseThrow(() -> Status.UNAVAILABLE.withDescription("Leader not in peer list").asRuntimeException());

    ClusterServiceGrpc.ClusterServiceBlockingStub stub = peerClients.blockingStub(leader);
    return stub.forwardJob(ForwardJobRequest.newBuilder().setSubmit(request).build());
  }
}

