package com.example.drm.node.raft;

import com.example.drm.node.config.NodeConfig;
import com.example.drm.proto.AppendEntriesRequest;
import com.example.drm.proto.AppendEntriesResponse;
import com.example.drm.proto.RequestVoteRequest;
import com.example.drm.proto.RequestVoteResponse;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Raft simplifié: élection, heartbeats, et leaderId.
 * <p>
 * On ne réplique pas (encore) un log complet: l'objectif ici est d'avoir un leader stable
 * et une vue cohérente du rôle/term côté dashboard + forwarding client.
 */
@Component
public class RaftNode {
  private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

  private final NodeConfig config;
  private final PeerClients peerClients;
  private final Clock clock = Clock.systemUTC();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "raft-" + System.nanoTime());
    t.setDaemon(true);
    return t;
  });

  private final Object lock = new Object();

  private volatile RaftRole role = RaftRole.FOLLOWER;
  private final AtomicLong term = new AtomicLong(0);
  private volatile String leaderId = null;
  private volatile String votedFor = null;
  private volatile long lastHeartbeatEpochMs = 0;

  private ScheduledFuture<?> electionTask;
  private ScheduledFuture<?> heartbeatTask;

  public RaftNode(NodeConfig config, PeerClients peerClients) {
    this.config = config;
    this.peerClients = peerClients;
    this.lastHeartbeatEpochMs = clock.millis();
    resetElectionTimer();
  }

  public String nodeId() {
    return config.nodeId();
  }

  public RaftRole role() {
    return role;
  }

  public long term() {
    return term.get();
  }

  public String leaderId() {
    return leaderId;
  }

  public long lastHeartbeatEpochMs() {
    return lastHeartbeatEpochMs;
  }

  public boolean isLeader() {
    return role == RaftRole.LEADER;
  }

  public List<Peer> peers() {
    List<Peer> peers = new ArrayList<>();
    for (var e : config.peers().entrySet()) {
      if (!Objects.equals(e.getKey(), config.nodeId())) {
        peers.add(new Peer(e.getKey(), e.getValue()));
      }
    }
    peers.sort(Comparator.comparing(Peer::nodeId));
    return peers;
  }

  public RequestVoteResponse onRequestVote(RequestVoteRequest req) {
    synchronized (lock) {
      if (req.getTerm() > term.get()) {
        becomeFollower(req.getTerm(), null);
      }

      boolean canVote = req.getTerm() == term.get()
          && (votedFor == null || votedFor.equals(req.getCandidateId()));
      if (canVote) {
        votedFor = req.getCandidateId();
        resetElectionTimer();
      }

      return RequestVoteResponse.newBuilder()
          .setTerm(term.get())
          .setVoteGranted(canVote)
          .build();
    }
  }

  public AppendEntriesResponse onAppendEntries(AppendEntriesRequest req) {
    synchronized (lock) {
      if (req.getTerm() < term.get()) {
        return AppendEntriesResponse.newBuilder()
            .setTerm(term.get())
            .setSuccess(false)
            .setMatchIndex(0)
            .build();
      }

      if (req.getTerm() > term.get() || role != RaftRole.FOLLOWER) {
        becomeFollower(req.getTerm(), req.getLeaderId());
      } else {
        leaderId = req.getLeaderId();
      }

      lastHeartbeatEpochMs = clock.millis();
      resetElectionTimer();

      return AppendEntriesResponse.newBuilder()
          .setTerm(term.get())
          .setSuccess(true)
          .setMatchIndex(0)
          .build();
    }
  }

  private void resetElectionTimer() {
    synchronized (lock) {
      if (electionTask != null) {
        electionTask.cancel(false);
      }
      long timeoutMs = ThreadLocalRandom.current().nextLong(1500, 3000);
      electionTask = scheduler.schedule(this::onElectionTimeout, timeoutMs, TimeUnit.MILLISECONDS);
    }
  }

  private void onElectionTimeout() {
    synchronized (lock) {
      if (role == RaftRole.LEADER) {
        resetElectionTimer();
        return;
      }
      startElection();
    }
  }

  private void startElection() {
    role = RaftRole.CANDIDATE;
    long newTerm = term.incrementAndGet();
    votedFor = config.nodeId();
    leaderId = null;
    lastHeartbeatEpochMs = clock.millis();

    log.info("Starting election (term={}, nodeId={})", newTerm, config.nodeId());

    int votes = 1;
    for (Peer peer : peers()) {
      try {
        RequestVoteResponse resp = peerClients.blockingStub(peer).requestVote(
            RequestVoteRequest.newBuilder()
                .setCandidateId(config.nodeId())
                .setTerm(newTerm)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build()
        );
        if (resp.getTerm() > term.get()) {
          becomeFollower(resp.getTerm(), null);
          resetElectionTimer();
          return;
        }
        if (resp.getVoteGranted()) {
          votes++;
        }
      } catch (StatusRuntimeException e) {
        log.debug("RequestVote failed to {} (nodeId={})", peer.nodeId(), config.nodeId(), e);
      }
    }

    if (votes > (peers().size() + 1) / 2) {
      becomeLeader();
    } else {
      role = RaftRole.FOLLOWER;
      resetElectionTimer();
    }
  }

  private void becomeLeader() {
    role = RaftRole.LEADER;
    leaderId = config.nodeId();
    votedFor = null;
    lastHeartbeatEpochMs = clock.millis();

    log.info("Became leader (term={}, nodeId={})", term.get(), config.nodeId());

    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
    }
    heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void becomeFollower(long newTerm, String newLeaderId) {
    role = RaftRole.FOLLOWER;
    term.set(newTerm);
    leaderId = newLeaderId;
    votedFor = null;
    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
      heartbeatTask = null;
    }
    lastHeartbeatEpochMs = clock.millis();
    log.info("Became follower (term={}, nodeId={}, leaderId={})", term.get(), config.nodeId(), leaderId);
  }

  private void sendHeartbeats() {
    if (!isLeader()) {
      return;
    }
    for (Peer peer : peers()) {
      try {
        peerClients.blockingStub(peer).appendEntries(
            AppendEntriesRequest.newBuilder()
                .setLeaderId(config.nodeId())
                .setTerm(term.get())
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build()
        );
      } catch (StatusRuntimeException e) {
        log.debug("AppendEntries failed to {} (leader={})", peer.nodeId(), config.nodeId(), e);
      }
    }
  }
}

