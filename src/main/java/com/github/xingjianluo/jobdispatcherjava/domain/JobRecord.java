package com.github.xingjianluo.jobdispatcherjava.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 作业的进程内状态记录（单进程内模拟集群作业实体）。
 * 非线程安全；并发访问由 {@code JobStore} 与调度器的锁保证。
 */
public final class JobRecord {

    private final String jobId;
    private final String idempotencyKey;
    private final String handlerType;
    private final String payload;
    private final Map<String, String> attributes;
    private final int priority;
    private final long submissionSeq;
    private final long createdAt;

    private JobStatus status;
    private FailureType failureType = FailureType.NONE;
    private String failureMessage;
    private int attempts;
    /** 当前租约的持有者；仅在 LEASED 状态非空。 */
    private String leaseOwner;
    /** 当前租约的代际，每次发放/重新分配递增；用于拒绝过期持有者的迟到回执。 */
    private long leaseEpoch;
    private long leaseExpiresAt;
    private long readyAt;
    private String resultPayload;
    private final List<StateEvent> events = new ArrayList<>();

    private JobRecord(String jobId, SubmitCommand cmd, long submissionSeq, long nowMillis) {
        this.jobId = jobId;
        this.idempotencyKey = cmd.idempotencyKey();
        this.handlerType = cmd.handlerType();
        this.payload = cmd.payload();
        this.attributes = cmd.attributes() == null ? Map.of() : Map.copyOf(cmd.attributes());
        this.priority = cmd.priority();
        this.submissionSeq = submissionSeq;
        this.createdAt = nowMillis;
        this.status = JobStatus.QUEUED;
        this.readyAt = nowMillis;
    }

    public static JobRecord newQueued(String jobId, SubmitCommand cmd, long submissionSeq, long nowMillis) {
        return new JobRecord(jobId, cmd, submissionSeq, nowMillis);
    }

    public String getJobId() { return jobId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getHandlerType() { return handlerType; }
    public String getPayload() { return payload; }
    public Map<String, String> getAttributes() { return attributes; }
    public int getPriority() { return priority; }
    public long getSubmissionSeq() { return submissionSeq; }
    public long getCreatedAt() { return createdAt; }
    public JobStatus getStatus() { return status; }
    public FailureType getFailureType() { return failureType; }
    public String getFailureMessage() { return failureMessage; }
    public int getAttempts() { return attempts; }
    public String getLeaseOwner() { return leaseOwner; }
    public long getLeaseEpoch() { return leaseEpoch; }
    public long getLeaseExpiresAt() { return leaseExpiresAt; }
    public long getReadyAt() { return readyAt; }
    public String getResultPayload() { return resultPayload; }
    public List<StateEvent> getEvents() { return events; }

    // ---- 以下变更方法供调度器在调度锁内调用（内部 API）：必须在调度锁内调用 ----

    /** 记录一次状态流转并附判定依据。 */
    public void appendEvent(long nowMillis, String reason) {
        JobStatus from = events.isEmpty() ? null : events.get(events.size() - 1).to();
        events.add(new StateEvent(nowMillis, from, this.status, reason));
    }

    /** 切换状态（同时由 {@link #appendEvent} 留下可观测轨迹）。 */
    public void transitionTo(JobStatus next, long nowMillis, String reason) {
        this.status = next;
        appendEvent(nowMillis, reason);
    }

    public void markLeased(String owner, long epoch, long expiresAt, long nowMillis, String reason) {
        this.leaseOwner = owner;
        this.leaseEpoch = epoch;
        this.leaseExpiresAt = expiresAt;
        this.attempts += 1;
        transitionTo(JobStatus.LEASED, nowMillis, reason);
    }

    public void renewLease(long expiresAt, long nowMillis, String reason) {
        this.leaseExpiresAt = expiresAt;
        appendEvent(nowMillis, reason);
    }

    public void releaseLease() {
        this.leaseOwner = null;
        this.leaseExpiresAt = 0L;
    }

    public void markSuccess(String resultPayload, long nowMillis, String reason) {
        this.resultPayload = resultPayload;
        this.failureType = FailureType.NONE;
        this.failureMessage = null;
        releaseLease();
        transitionTo(JobStatus.SUCCEEDED, nowMillis, reason);
    }

    public void markNonRetryableFailure(String message, long nowMillis, String reason) {
        this.failureType = FailureType.NON_RETRYABLE;
        this.failureMessage = message;
        releaseLease();
        transitionTo(JobStatus.FAILED, nowMillis, reason);
    }

    public void markWaitingRetry(FailureType failureType, String message, long readyAt,
                          long nowMillis, String reason) {
        this.failureType = failureType;
        this.failureMessage = message;
        this.readyAt = readyAt;
        releaseLease();
        transitionTo(JobStatus.WAITING_RETRY, nowMillis, reason);
    }

    public void markExhausted(FailureType failureType, String message, long nowMillis, String reason) {
        this.failureType = failureType;
        this.failureMessage = message;
        releaseLease();
        transitionTo(JobStatus.EXHAUSTED, nowMillis, reason);
    }

    public void markCancelled(long nowMillis, String reason) {
        this.failureType = FailureType.CANCELLED;
        releaseLease();
        transitionTo(JobStatus.CANCELLED, nowMillis, reason);
    }

    public void setReadyAt(long readyAt) {
        this.readyAt = readyAt;
    }
}
