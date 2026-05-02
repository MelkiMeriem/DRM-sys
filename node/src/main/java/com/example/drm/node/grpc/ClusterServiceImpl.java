package com.example.drm.node.grpc;

import com.example.drm.node.config.NodeConfig;
import com.example.drm.node.jobs.JobManager;
import com.example.drm.node.raft.RaftNode;
import com.example.drm.proto.AppendEntriesRequest;
import com.example.drm.proto.AppendEntriesResponse;
import com.example.drm.proto.ClusterServiceGrpc;
import com.example.drm.proto.ForwardJobRequest;
import com.example.drm.proto.ForwardJobResponse;
import com.example.drm.proto.GetNodeStatusRequest;
import com.example.drm.proto.GetNodeStatusResponse;
import com.example.drm.proto.Job;
import com.example.drm.proto.RequestVoteRequest;
import com.example.drm.proto.RequestVoteResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {
  private final NodeConfig config;
  private final RaftNode raft;
  private final JobManager jobManager;

  public ClusterServiceImpl(NodeConfig config, RaftNode raft, JobManager jobManager) {
    this.config = config;
    this.raft = raft;
    this.jobManager = jobManager;
  }

  @Override
  public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
    responseObserver.onNext(raft.onRequestVote(request));
    responseObserver.onCompleted();
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
    responseObserver.onNext(raft.onAppendEntries(request));
    responseObserver.onCompleted();
  }

  @Override
  public void forwardJob(ForwardJobRequest request, StreamObserver<ForwardJobResponse> responseObserver) {
    if (!raft.isLeader()) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription("Not leader")
              .augmentDescription("leaderId=" + Optional.ofNullable(raft.leaderId()).orElse(""))
              .asRuntimeException()
      );
      return;
    }

    Job job = jobManager.submit(request.getSubmit().getType(), request.getSubmit().getPayloadJson(), config.nodeId());
    responseObserver.onNext(ForwardJobResponse.newBuilder()
        .setSubmit(com.example.drm.proto.SubmitJobResponse.newBuilder().setJob(job).setLeaderNodeId(config.nodeId()).build())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getNodeStatus(GetNodeStatusRequest request, StreamObserver<GetNodeStatusResponse> responseObserver) {
    responseObserver.onNext(GetNodeStatusResponse.newBuilder()
        .setNodeId(config.nodeId())
        .setRole(raft.role().name())
        .setTerm(raft.term())
        .setLeaderId(Optional.ofNullable(raft.leaderId()).orElse(""))
        .setLastHeartbeatEpochMs(raft.lastHeartbeatEpochMs())
        .setLogLastIndex(0)
        .setCommitIndex(0)
        .setJobsTotal(jobManager.jobsTotal())
        .setJobsRunning(jobManager.jobsRunning())
        .build());
    responseObserver.onCompleted();
  }
}

