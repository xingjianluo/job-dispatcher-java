package com.github.xingjianluo.jobdispatcherjava.job;

import java.time.Instant;

/**
 * 作业实体。所有状态变更必须通过 JobDispatcher 完成。
 */
public class Job {

    private final String id;
    private final String idempotencyKey;
    private final int priority;
    private final String payload;
    private final int maxAttempts;
    private final Instant createdAt;

    private volatile JobStatus status;
    private volatile int attempt;
    private volatile String leaseOwner;
    private volatile String leaseId;
    private volatile Instant leaseExpiresAt;
    private volatile Instant nextAttemptAt;
    private volatile FailureKind failureKind;
    private volatile String failureMessage;
    private volatile long version;

    public Job(String id, String idempotencyKey, int priority, String payload, int maxAttempts, Instant createdAt) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.priority = priority;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.createdAt = createdAt;
        this.status = JobStatus.QUEUED;
        this.attempt = 0;
        this.nextAttemptAt = createdAt;
        this.version = 0;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public int getPriority() { return priority; }
    public String getPayload() { return payload; }
    public int getMaxAttempts() { return maxAttempts; }
    public Instant getCreatedAt() { return createdAt; }
    public JobStatus getStatus() { return status; }
    public int getAttempt() { return attempt; }
    public String getLeaseOwner() { return leaseOwner; }
    public String getLeaseId() { return leaseId; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public FailureKind getFailureKind() { return failureKind; }
    public String getFailureMessage() { return failureMessage; }
    public long getVersion() { return version; }

    void setStatus(JobStatus status) { this.status = status; }
    void setAttempt(int attempt) { this.attempt = attempt; }
    void setLeaseOwner(String leaseOwner) { this.leaseOwner = leaseOwner; }
    void setLeaseId(String leaseId) { this.leaseId = leaseId; }
    void setLeaseExpiresAt(Instant leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    void setFailureKind(FailureKind failureKind) { this.failureKind = failureKind; }
    void setFailureMessage(String failureMessage) { this.failureMessage = failureMessage; }
    void setVersion(long version) { this.version = version; }
}
