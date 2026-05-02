package com.example.drm.node.jobs;

import com.example.drm.proto.Job;
import com.example.drm.proto.JobStatus;
import com.example.drm.proto.JobType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class JobManager {
  private static final Logger log = LoggerFactory.getLogger(JobManager.class);

  private final Clock clock = Clock.systemUTC();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ExecutorService executor = Executors.newFixedThreadPool(4);

  private final Map<String, Job> jobsById = new ConcurrentHashMap<>();
  private final AtomicInteger running = new AtomicInteger(0);

  public Job submit(JobType type, String payloadJson, String executedByNodeId) {
    String jobId = UUID.randomUUID().toString();
    long now = clock.millis();

    Job job = Job.newBuilder()
        .setJobId(jobId)
        .setType(type)
        .setPayloadJson(payloadJson == null ? "" : payloadJson)
        .setStatus(JobStatus.JOB_STATUS_PENDING)
        .setCreatedAtEpochMs(now)
        .setExecutedByNodeId(executedByNodeId)
        .build();

    jobsById.put(jobId, job);

    executor.submit(() -> runJob(jobId));
    return job;
  }

  public Job get(String jobId) {
    return jobsById.get(jobId);
  }

  public int jobsTotal() {
    return jobsById.size();
  }

  public int jobsRunning() {
    return running.get();
  }

  private void runJob(String jobId) {
    Job job = jobsById.get(jobId);
    if (job == null) {
      return;
    }

    running.incrementAndGet();
    jobsById.computeIfPresent(jobId, (id, old) -> old.toBuilder().setStatus(JobStatus.JOB_STATUS_RUNNING).build());

    try {
      String resultJson = switch (job.getType()) {
        case JOB_TYPE_SUM -> sum(job.getPayloadJson());
        case JOB_TYPE_SHA256 -> sha256(job.getPayloadJson());
        case JOB_TYPE_MESSAGE -> message(job.getPayloadJson());
        default -> throw new IllegalArgumentException("Unsupported job type: " + job.getType());
      };

      long finished = clock.millis();
      jobsById.computeIfPresent(jobId, (id, old) -> old.toBuilder()
          .setStatus(JobStatus.JOB_STATUS_SUCCEEDED)
          .setResultJson(resultJson)
          .setFinishedAtEpochMs(finished)
          .build());
    } catch (Exception e) {
      long finished = clock.millis();
      log.warn("Job failed (jobId={})", jobId, e);
      jobsById.computeIfPresent(jobId, (id, old) -> old.toBuilder()
          .setStatus(JobStatus.JOB_STATUS_FAILED)
          .setErrorMessage(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())
          .setFinishedAtEpochMs(finished)
          .build());
    } finally {
      running.decrementAndGet();
    }
  }

  private String sum(String payloadJson) throws Exception {
    JsonNode root = objectMapper.readTree(payloadJson);
    JsonNode nums = root.get("numbers");
    if (nums == null || !nums.isArray()) {
      throw new IllegalArgumentException("payload_json must contain array field 'numbers'");
    }
    long sum = 0;
    for (JsonNode n : nums) {
      sum += n.asLong();
    }
    return objectMapper.createObjectNode().put("sum", sum).toString();
  }

  private String sha256(String payloadJson) throws Exception {
    JsonNode root = objectMapper.readTree(payloadJson);
    String text = root.path("text").asText(null);
    if (text == null) {
      throw new IllegalArgumentException("payload_json must contain field 'text'");
    }
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] digest = md.digest(text.getBytes(StandardCharsets.UTF_8));
    String hex = HexFormat.of().formatHex(digest);
    return objectMapper.createObjectNode().put("sha256", hex).toString();
  }

  private String message(String payloadJson) throws Exception {
    JsonNode root = objectMapper.readTree(payloadJson);
    String msg = root.path("message").asText(null);
    if (msg == null) {
      throw new IllegalArgumentException("payload_json must contain field 'message'");
    }
    return objectMapper.createObjectNode()
        .put("accepted", true)
        .put("echo", msg)
        .toString();
  }
}

