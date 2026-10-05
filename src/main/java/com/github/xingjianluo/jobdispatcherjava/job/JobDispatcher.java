package com.github.xingjianluo.jobdispatcherjava.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地作业调度与执行服务：进程内模拟集群任务分发。
 *
 * <p>并发模型：所有状态变更都在同一把锁内完成，因此
 * 同一作业任意时刻至多被一个执行方持有租约，并发抢占不会产生重复执行。</p>
 *
 * <p>调度语义：优先级高者先调度，同优先级按提交时间先进先出；
 * 容量不足时新提交被拒绝并给出原因；执行容量不足时作业留在队列中等待。</p>
 */
@Service
public class JobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcher.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final JobDispatcherProperties properties;
    private final Clock clock;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, String> idempotencyIndex = new HashMap<>();
    /** 每个作业的状态流转记录，按发生顺序追加；仅在持锁时写入 */
    private final Map<String, List<JobEvent>> histories = new HashMap<>();
    /** 全局事件序号，保证并发/连续流转时顺序可校验 */
    private long eventSequence = 0;
    /** 已到期可执行的排队作业：优先级降序，同优先级按创建时间升序 */
    private final PriorityQueue<Job> readyQueue = new PriorityQueue<>(
            Comparator.comparingInt(Job::getPriority).reversed()
                    .thenComparing(Job::getCreatedAt)
                    .thenComparing(Job::getId));

    @Autowired
    public JobDispatcher(JobDispatcherProperties properties) {
        this(properties, Clock.systemUTC());
    }

    public JobDispatcher(JobDispatcherProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    /** 提交作业（幂等）：同一幂等标识重复提交返回既有作业，不产生多次执行。 */
    public SubmitResult submit(String idempotencyKey, int priority, String payload) {
        lock.lock();
        try {
            String existingId = idempotencyIndex.get(idempotencyKey);
            if (existingId != null) {
                Job existing = jobs.get(existingId);
                SubmitResult result = SubmitResult.duplicate(existing);
                log.info("[submit] jobId={} key={} outcome={} reason={}",
                        existing.getId(), idempotencyKey, result.outcome(), result.reason());
                return result;
            }
            if (activeJobCount() >= properties.getMaxQueueCapacity()) {
                SubmitResult result = SubmitResult.rejected(idempotencyKey, properties.getMaxQueueCapacity());
                log.info("[submit] key={} outcome={} reason={}", idempotencyKey, result.outcome(), result.reason());
                return result;
            }
            Job job = new Job(UUID.randomUUID().toString(), idempotencyKey, priority, payload,
                    properties.getMaxAttempts(), Instant.now(clock));
            jobs.put(job.getId(), job);
            idempotencyIndex.put(idempotencyKey, job.getId());
            readyQueue.add(job);
            record(job, null, JobStatus.QUEUED, "system",
                    "submitted and enqueued priority=" + priority);
            SubmitResult result = SubmitResult.accepted(job);
            log.info("[submit] jobId={} key={} priority={} outcome={} reason={}",
                    job.getId(), idempotencyKey, priority, result.outcome(), result.reason());
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行方尝试获取一个可执行作业的租约。
     * 执行容量不足或无到期作业时返回未获取及原因，作业留在队列中等待。
     */
    public AcquireResult acquire(String owner) {
        lock.lock();
        try {
            promoteDueJobs();
            if (leasedCount() >= properties.getMaxConcurrent()) {
                log.info("[acquire] owner={} acquired=false reason=concurrency capacity {} reached",
                        owner, properties.getMaxConcurrent());
                return AcquireResult.empty("concurrency capacity " + properties.getMaxConcurrent() + " reached");
            }
            Job job = readyQueue.poll();
            if (job == null) {
                log.info("[acquire] owner={} acquired=false reason=no queued job due", owner);
                return AcquireResult.empty("no queued job due");
            }
            String leaseId = UUID.randomUUID().toString();
            job.setAttempt(job.getAttempt() + 1);
            job.setLeaseOwner(owner);
            job.setLeaseId(leaseId);
            job.setLeaseExpiresAt(Instant.now(clock).plus(properties.getLeaseDuration()));
            transition(job, JobStatus.LEASED, owner, "lease granted to " + owner
                    + " attempt=" + job.getAttempt()
                    + " expiresAt=" + job.getLeaseExpiresAt());
            return AcquireResult.acquired(job, leaseId);
        } finally {
            lock.unlock();
        }
    }

    /** 续期租约；租约不匹配（已被抢占/回收/取消）时抛内部冲突异常。 */
    public boolean renew(String jobId, String leaseId, String owner) {
        lock.lock();
        try {
            Job job = requireLeasedBy(jobId, leaseId, owner, "renew");
            job.setLeaseExpiresAt(Instant.now(clock).plus(properties.getLeaseDuration()));
            bump(job);
            record(job, JobStatus.LEASED, JobStatus.LEASED, owner,
                    "lease renewed by " + owner + " newExpiresAt=" + job.getLeaseExpiresAt());
            log.info("[renew] jobId={} owner={} newExpiresAt={}", jobId, owner, job.getLeaseExpiresAt());
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 报告执行成功，进入终态 SUCCEEDED。 */
    public void complete(String jobId, String leaseId, String owner) {
        lock.lock();
        try {
            Job job = requireLeasedBy(jobId, leaseId, owner, "complete");
            clearLease(job);
            transition(job, JobStatus.SUCCEEDED, owner, "completed by " + owner);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 报告执行失败。
     * retryable 且尝试次数未耗尽：退避后重新入队；
     * 否则进入终态 FAILED，失败分类为 NON_RETRYABLE 或 RETRIES_EXHAUSTED。
     */
    public void fail(String jobId, String leaseId, String owner, boolean retryable, String message) {
        lock.lock();
        try {
            Job job = requireLeasedBy(jobId, leaseId, owner, "fail");
            clearLease(job);
            job.setFailureMessage(message);
            if (retryable && job.getAttempt() < job.getMaxAttempts()) {
                Duration backoff = retryBackoff(job.getAttempt());
                job.setNextAttemptAt(Instant.now(clock).plus(backoff));
                transition(job, JobStatus.QUEUED, owner, "retryable failure, backoff=" + backoff
                        + " attempt=" + job.getAttempt() + "/" + job.getMaxAttempts()
                        + " message=" + message);
            } else {
                FailureKind kind = retryable ? FailureKind.RETRIES_EXHAUSTED : FailureKind.NON_RETRYABLE;
                job.setFailureKind(kind);
                transition(job, JobStatus.FAILED, owner, "terminal failure kind=" + kind
                        + " attempt=" + job.getAttempt() + "/" + job.getMaxAttempts()
                        + " message=" + message);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取消作业。
     *
     * @return true 本次取消生效；false 作业已处于终态（重复取消可判别）
     */
    public boolean cancel(String jobId) {
        return cancelWithOutcome(jobId).effective();
    }

    /**
     * 取消作业（三态结果）。
     *
     * <p>CANCELLED：本次生效；ALREADY_CANCELLED：此前已被取消过；
     * ALREADY_TERMINAL：已因其它原因进入终态。后两种不改变作业原有
     * 状态与失败原因，调用方可直接区分。</p>
     */
    public CancelResult cancelWithOutcome(String jobId) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                throw new JobConflictException("cancel rejected: unknown jobId=" + jobId);
            }
            if (job.getStatus() == JobStatus.CANCELLED) {
                CancelResult result = CancelResult.alreadyCancelled(job);
                log.info("[cancel] jobId={} outcome={} reason={}", jobId, result.outcome(), result.reason());
                return result;
            }
            if (isTerminal(job.getStatus())) {
                CancelResult result = CancelResult.alreadyTerminal(job);
                log.info("[cancel] jobId={} outcome={} status={} failureKind={} reason={}",
                        jobId, result.outcome(), job.getStatus(), job.getFailureKind(), result.reason());
                return result;
            }
            if (job.getStatus() == JobStatus.QUEUED) {
                readyQueue.remove(job);
            }
            clearLease(job);
            job.setFailureKind(FailureKind.CANCELLED);
            transition(job, JobStatus.CANCELLED, "system", "cancelled");
            CancelResult result = CancelResult.cancelled(job);
            log.info("[cancel] jobId={} outcome={} reason={}", jobId, result.outcome(), result.reason());
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 回收到期租约：执行方失联的租约到期后纳入同一套有界重试。
     *
     * <p>每次回收消耗一次尝试机会（获取租约时已计入 attempt），并按退避策略
     * 安排下一次可被抢占的时间，不会刚回收就立刻被抢走空转；
     * 尝试次数耗尽时进入终态 FAILED/LOST_RETRIES_EXHAUSTED，
     * 与 RETRIES_EXHAUSTED、NON_RETRYABLE 可区分，不会无限循环或永久卡死。</p>
     *
     * @return 被回收的作业数（含因耗尽而进入终态的）
     */
    public int reapExpiredLeases() {
        lock.lock();
        try {
            Instant now = Instant.now(clock);
            int reaped = 0;
            for (Job job : jobs.values()) {
                if (job.getStatus() == JobStatus.LEASED
                        && job.getLeaseExpiresAt() != null
                        && !job.getLeaseExpiresAt().isAfter(now)) {
                    String lostOwner = job.getLeaseOwner();
                    clearLease(job);
                    reaped++;
                    if (job.getAttempt() >= job.getMaxAttempts()) {
                        job.setFailureKind(FailureKind.LOST_RETRIES_EXHAUSTED);
                        job.setFailureMessage("lost contact with owner " + lostOwner
                                + ", attempts exhausted");
                        transition(job, JobStatus.FAILED, "system",
                                "lease expired, reclaimed from lost owner=" + lostOwner
                                        + ", attempts exhausted " + job.getAttempt() + "/"
                                        + job.getMaxAttempts()
                                        + ", terminal kind=LOST_RETRIES_EXHAUSTED");
                    } else {
                        Duration backoff = retryBackoff(job.getAttempt());
                        job.setNextAttemptAt(now.plus(backoff));
                        transition(job, JobStatus.QUEUED, "system",
                                "lease expired, reclaimed from lost owner=" + lostOwner
                                        + ", backoff=" + backoff
                                        + " attempt=" + job.getAttempt() + "/" + job.getMaxAttempts()
                                        + " nextAttemptAt=" + job.getNextAttemptAt());
                    }
                }
            }
            if (reaped > 0) {
                log.info("[reap] reaped={} expired lease(s)", reaped);
            }
            return reaped;
        } finally {
            lock.unlock();
        }
    }

    public Optional<Job> find(String jobId) {
        lock.lock();
        try {
            return Optional.ofNullable(jobs.get(jobId));
        } finally {
            lock.unlock();
        }
    }

    public List<Job> listJobs() {
        lock.lock();
        try {
            return new ArrayList<>(jobs.values());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查询作业从提交到当前的状态流转记录，按发生顺序（sequence 升序）返回。
     * 未知作业返回空列表。
     */
    public List<JobEvent> history(String jobId) {
        lock.lock();
        try {
            return List.copyOf(histories.getOrDefault(jobId, List.of()));
        } finally {
            lock.unlock();
        }
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 MAX_BACKOFF。 */
    Duration retryBackoff(int attempt) {
        long baseMillis = properties.getRetryBackoffBase().toMillis();
        long shifted = baseMillis << Math.min(attempt - 1, 20);
        return shifted >= MAX_BACKOFF.toMillis() ? MAX_BACKOFF : Duration.ofMillis(shifted);
    }

    // ---- 内部辅助 ----

    /** 把退避到期的 QUEUED 作业放回就绪队列。 */
    private void promoteDueJobs() {
        Instant now = Instant.now(clock);
        for (Job job : jobs.values()) {
            if (job.getStatus() == JobStatus.QUEUED
                    && job.getNextAttemptAt() != null
                    && !job.getNextAttemptAt().isAfter(now)
                    && !readyQueue.contains(job)) {
                readyQueue.add(job);
            }
        }
    }

    private Job requireLeasedBy(String jobId, String leaseId, String owner, String action) {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new JobConflictException(action + " rejected: unknown jobId=" + jobId);
        }
        if (job.getStatus() != JobStatus.LEASED
                || !leaseId.equals(job.getLeaseId())
                || !owner.equals(job.getLeaseOwner())) {
            throw new JobConflictException(action + " rejected: lease mismatch for jobId=" + jobId
                    + " status=" + job.getStatus() + " (lease lost, job not executed twice)");
        }
        return job;
    }

    private void transition(Job job, JobStatus to, String actor, String rationale) {
        JobStatus from = job.getStatus();
        job.setStatus(to);
        bump(job);
        record(job, from, to, actor, rationale);
    }

    /** 追加一条流转记录；必须在持锁状态下调用，保证序号与状态一致、不丢不重。 */
    private void record(Job job, JobStatus from, JobStatus to, String actor, String rationale) {
        JobEvent event = new JobEvent(++eventSequence, job.getId(), Instant.now(clock),
                from, to, job.getAttempt(), actor, rationale);
        histories.computeIfAbsent(job.getId(), k -> new ArrayList<>()).add(event);
        log.info("[transition] seq={} jobId={} key={} {} -> {} version={} actor={} reason={}",
                event.sequence(), job.getId(), job.getIdempotencyKey(), from, to,
                job.getVersion(), actor, rationale);
    }

    private void bump(Job job) {
        job.setVersion(job.getVersion() + 1);
    }

    private void clearLease(Job job) {
        job.setLeaseOwner(null);
        job.setLeaseId(null);
        job.setLeaseExpiresAt(null);
    }

    private int leasedCount() {
        int count = 0;
        for (Job job : jobs.values()) {
            if (job.getStatus() == JobStatus.LEASED) {
                count++;
            }
        }
        return count;
    }

    private int activeJobCount() {
        int count = 0;
        for (Job job : jobs.values()) {
            if (!isTerminal(job.getStatus())) {
                count++;
            }
        }
        return count;
    }

    private boolean isTerminal(JobStatus status) {
        return status == JobStatus.SUCCEEDED || status == JobStatus.FAILED || status == JobStatus.CANCELLED;
    }
}
