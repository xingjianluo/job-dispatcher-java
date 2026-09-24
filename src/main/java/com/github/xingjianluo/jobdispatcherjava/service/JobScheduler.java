package com.github.xingjianluo.jobdispatcherjava.service;

import com.github.xingjianluo.jobdispatcherjava.config.Clock;
import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.FailureType;
import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.domain.JobStatus;
import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import com.github.xingjianluo.jobdispatcherjava.domain.SubmitCommand;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandler;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandlerRegistry;
import com.github.xingjianluo.jobdispatcherjava.store.JobStore;
import com.github.xingjianluo.jobdispatcherjava.web.JobNotFoundException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内作业调度器：单把调度锁串行化所有状态流转，模拟集群中对共享作业表的原子抢占。
 *
 * <p>关键不变量：
 * <ul>
 *   <li>发放租约（QUEUED/WAITING_RETRY -&gt; LEASED）只在调度锁内发生，同一作业任意时刻至多一个持有者；</li>
 *   <li>每次发放递增 leaseEpoch，过期持有者的迟到心跳/回执按代际拒绝；</li>
 *   <li>容量（在途租约数）在锁内核验，容量不足只排队，不超发、不丢弃；</li>
 *   <li>租约过期按一次可重试失败计数，超过上限进入 EXHAUSTED，不会永久卡住。</li>
 * </ul>
 */
@Service
public class JobScheduler implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final JobStore store;
    private final JobHandlerRegistry handlers;
    private final DispatcherProperties props;
    private final RetryPolicy retryPolicy;
    private final Clock clock;

    /** 调度锁：保护作业状态流转与容量计数。回调具体执行器时不持锁。 */
    private final Object scheduleLock = new Object();
    private final AtomicLong submissionSeq = new AtomicLong();
    private final AtomicInteger workerNameSeq = new AtomicInteger();

    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService heartbeatExecutor;
    private ExecutorService workers;
    private volatile boolean running;

    public JobScheduler(JobStore store,
                        JobHandlerRegistry handlers,
                        DispatcherProperties props,
                        RetryPolicy retryPolicy,
                        Clock clock) {
        this.store = store;
        this.handlers = handlers;
        this.props = props;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
    }

    /** 供测试构造：使用自定义重试策略与受控时钟（例如秒级以下租约、手动推进时间）。 */
    public static JobScheduler forTesting(JobStore store,
                                          JobHandlerRegistry handlers,
                                          DispatcherProperties props,
                                          RetryPolicy retryPolicy,
                                          Clock clock) {
        return new JobScheduler(store, handlers, props, retryPolicy, clock);
    }

    /** 测试/本地控制入口：显式启动后台调度。 */
    @PostConstruct
    public void start() {
        synchronized (scheduleLock) {
            if (running) {
                return;
            }
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "job-dispatcher-scheduler");
                t.setDaemon(true);
                return t;
            };
            scheduler = Executors.newSingleThreadScheduledExecutor(tf);
            ThreadFactory hb = r -> {
                Thread t = new Thread(r, "job-dispatcher-heartbeat");
                t.setDaemon(true);
                return t;
            };
            heartbeatExecutor = Executors.newScheduledThreadPool(1, hb);
            ThreadFactory wf = r -> {
                Thread t = new Thread(r, "job-worker-" + workerNameSeq.incrementAndGet());
                t.setDaemon(true);
                return t;
            };
            // 工作池大小等于容量上限，叠加调度锁内的在途计数，形成双保险，绝不超发
            workers = Executors.newFixedThreadPool(props.maxConcurrent(), wf);
            running = true;
            scheduler.scheduleWithFixedDelay(this::safeDispatchLoop,
                    0, props.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
            scheduler.scheduleWithFixedDelay(this::safeSweep,
                    props.sweepInterval().toMillis(), props.sweepInterval().toMillis(),
                    TimeUnit.MILLISECONDS);
            log.info("调度器已启动: 容量={} 队列上限={} 租约={} 最大尝试次数={}",
                    props.maxConcurrent(), props.maxQueueSize(), props.leaseDuration(),
                    retryPolicy.maxAttempts());
        }
    }

    /** 测试/本地控制入口：显式停止。 */
    public void shutdown() {
        synchronized (scheduleLock) {
            if (!running) {
                return;
            }
            running = false;
        }
        scheduler.shutdownNow();
        heartbeatExecutor.shutdownNow();
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(2, TimeUnit.SECONDS)) {
                log.warn("停止时仍有执行方未结束");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("调度器已停止");
    }

    @Override
    public void destroy() {
        shutdown();
    }

    /** 提交作业（幂等）。 */
    public SubmitResult submit(SubmitCommand command) {
        // 幂等优先：包括终态作业在内都永不二次执行
        Optional<JobRecord> duplicate = store.findByIdempotencyKey(command.idempotencyKey());
        if (duplicate.isPresent()) {
            JobRecord existing = duplicate.get();
            log.info("提交幂等命中 jobId={} idempotencyKey={} 当前状态={} -> 判定 DUPLICATE_SUBMISSION",
                    existing.getJobId(), command.idempotencyKey(), existing.getStatus());
            return new SubmitResult.Duplicate(existing);
        }

        // 处理器类型在入队前校验，避免注定无法执行的作业占用队列
        if (handlers.find(command.handlerType()).isEmpty()) {
            long now = clock.nowMillis();
            JobRecord rejected = JobRecord.newQueued(
                    UUID.randomUUID().toString(), command, submissionSeq.incrementAndGet(), now);
            log.info("提交被拒 idempotencyKey={} handlerType={} -> 判定 UNKNOWN_HANDLER（作业未入队）",
                    command.idempotencyKey(), command.handlerType());
            return new SubmitResult.RejectedUnknownHandler(rejected, command.handlerType());
        }

        // 在调度锁内二次校验：判重、队列容量、创建三者原子完成
        JobRecord accepted;
        synchronized (scheduleLock) {
            JobRecord existing = store.findByIdempotencyKey(command.idempotencyKey()).orElse(null);
            if (existing != null) {
                log.info("提交幂等命中(锁内) jobId={} idempotencyKey={} 状态={} -> DUPLICATE_SUBMISSION",
                        existing.getJobId(), command.idempotencyKey(), existing.getStatus());
                return new SubmitResult.Duplicate(existing);
            }
            int waiting = countWaiting();
            if (waiting >= props.maxQueueSize()) {
                long now = clock.nowMillis();
                JobRecord rejected = JobRecord.newQueued(
                        UUID.randomUUID().toString(), command, submissionSeq.incrementAndGet(), now);
                log.info("提交被拒 idempotencyKey={} 等待队列长度={}/{} -> 判定 QUEUE_FULL（作业未入队）",
                        command.idempotencyKey(), waiting, props.maxQueueSize());
                return new SubmitResult.RejectedQueueFull(rejected, waiting);
            }

            long now = clock.nowMillis();
            accepted = store.putIfAbsent(command.idempotencyKey(), key -> {
                JobRecord created = JobRecord.newQueued(
                        UUID.randomUUID().toString(), command, submissionSeq.incrementAndGet(), now);
                created.appendEvent(now, "作业提交入队：幂等键=%s，优先级=%d，容量=%d/%d"
                        .formatted(key, command.priority(), countLeases(), props.maxConcurrent()));
                return created;
            });

            int queuePosition = queuePositionOf(accepted);
            log.info("作业已接收 jobId={} idempotencyKey={} 优先级={} 排队位置={}/{} 在途={}/{}",
                    accepted.getJobId(), command.idempotencyKey(), command.priority(),
                    queuePosition, countWaiting(), countLeases(), props.maxConcurrent());
        }

        // 提交后立即尝试调度（不持锁），降低排队等待延迟
        safeDispatchLoop();
        synchronized (scheduleLock) {
            JobRecord snapshot = store.findByIdempotencyKey(command.idempotencyKey()).orElseThrow();
            return new SubmitResult.Accepted(snapshot, queuePositionOf(snapshot),
                    countWaiting(), countLeases(), props.maxConcurrent());
        }
    }

    /** 取消作业。 */
    public CancelResult cancel(String jobId) {
        synchronized (scheduleLock) {
            JobRecord job = store.findByJobId(jobId)
                    .orElseThrow(() -> new JobNotFoundException(jobId));
            JobStatus status = job.getStatus();
            if (status == JobStatus.CANCELLED) {
                log.info("取消请求 jobId={} 状态=CANCELLED -> 重复取消，忽略", jobId);
                return new CancelResult.AlreadyCancelled(job);
            }
            if (status.isTerminal()) {
                log.info("取消请求 jobId={} 状态={} -> 终态不可取消", jobId, status);
                return new CancelResult.Terminal(job);
            }
            long now = clock.nowMillis();
            if (status == JobStatus.LEASED) {
                job.markCancelled(now, "取消时作业在执行中：回收租约 owner=%s epoch=%d，执行方心跳/回执将被拒绝"
                        .formatted(job.getLeaseOwner(), job.getLeaseEpoch()));
            } else {
                job.markCancelled(now, "取消时作业处于%s：直接取消，不执行".formatted(status));
            }
            logTransition(job, "取消生效");
        }
        safeDispatchLoop();
        return new CancelResult.Cancelled(store.findByJobId(jobId).orElseThrow());
    }

    public Optional<JobRecord> get(String jobId) {
        return store.findByJobId(jobId);
    }

    public Collection<JobRecord> list() {
        return store.findAll();
    }

    /** 执行方心跳续期。返回 false 表示租约已失效（被取消或过期重分配），执行方必须停手。 */
    public boolean heartbeat(String jobId, String workerId, long leaseEpoch) {
        synchronized (scheduleLock) {
            JobRecord job = store.findByJobId(jobId).orElse(null);
            if (job == null
                    || job.getStatus() != JobStatus.LEASED
                    || job.getLeaseEpoch() != leaseEpoch
                    || !workerId.equals(job.getLeaseOwner())) {
                log.warn("心跳拒绝 jobId={} worker={} epoch={} 实际状态={}/owner={}/epoch={}",
                        jobId, workerId, leaseEpoch,
                        job == null ? null : job.getStatus(),
                        job == null ? null : job.getLeaseOwner(),
                        job == null ? null : job.getLeaseEpoch());
                return false;
            }
            job.renewLease(clock.nowMillis() + props.leaseDuration().toMillis(), clock.nowMillis(),
                    "心跳续期：owner=%s epoch=%d".formatted(workerId, leaseEpoch));
            return true;
        }
    }

    /** 执行方回执（仅同代际持有者有效）。 */
    public void complete(String jobId, String workerId, long leaseEpoch,
                         WorkerTask.AttemptResult result) {
        synchronized (scheduleLock) {
            JobRecord job = store.findByJobId(jobId).orElse(null);
            if (job == null
                    || job.getStatus() != JobStatus.LEASED
                    || job.getLeaseEpoch() != leaseEpoch
                    || !workerId.equals(job.getLeaseOwner())) {
                // 过期持有者的迟到回执：只记录、不生效，防止重复/越代执行污染状态
                log.warn("回执拒绝 jobId={} worker={} epoch={} success={} 实际状态={}/owner={}/epoch={}（迟到或越代结果已丢弃）",
                        jobId, workerId, leaseEpoch, result.success(),
                        job == null ? null : job.getStatus(),
                        job == null ? null : job.getLeaseOwner(),
                        job == null ? null : job.getLeaseEpoch());
                return;
            }
            long now = clock.nowMillis();
            if (result.success()) {
                job.markSuccess(result.message(), now,
                        "第%d次尝试成功：owner=%s".formatted(job.getAttempts(), workerId));
                logTransition(job, "执行成功");
            } else if (!result.retryable()) {
                job.markNonRetryableFailure(result.message(), now,
                        "第%d次尝试不可重试失败，直接进入 FAILED：owner=%s 原因=%s"
                                .formatted(job.getAttempts(), workerId, result.message()));
                logTransition(job, "不可重试失败");
            } else {
                scheduleRetryOrExhaust(job, FailureType.RETRYABLE, result.message(), now,
                        "第%d次尝试可重试失败：owner=%s 原因=%s"
                                .formatted(job.getAttempts(), workerId, result.message()));
            }
        }
        // 成功/失败释放出容量后立即补调度
        safeDispatchLoop();
    }

    /** 触发一次调度扫描（后台轮询与提交后立即触发共用）。 */
    void dispatchLoop() {
        List<Dispatched> picked = new ArrayList<>();
        synchronized (scheduleLock) {
            long now = clock.nowMillis();
            int active = countLeases();
            while (active < props.maxConcurrent()) {
                JobRecord next = pickNext(now);
                if (next == null) {
                    break;
                }
                String workerId = "worker-" + workerNameSeq.incrementAndGet();
                long epoch = next.getLeaseEpoch() + 1;
                int attempt = next.getAttempts() + 1;
                long expiresAt = now + props.leaseDuration().toMillis();
                String fromText = next.getStatus() == JobStatus.WAITING_RETRY
                        ? "退避到期重新调度（第%d次尝试）".formatted(attempt)
                        : "容量可用，按调度顺序发放租约（第%d次尝试）".formatted(attempt);
                next.markLeased(workerId, epoch, expiresAt, now,
                        fromText + "：owner=%s，租约到期=%d".formatted(workerId, expiresAt));
                logTransition(next, "租约发放");
                active++;
                picked.add(new Dispatched(next, workerId, attempt, epoch));
            }
        }
        // 提交执行任务到工作池必须在锁外：工作池任务不与调度锁形成嵌套依赖
        for (Dispatched d : picked) {
            JobHandler handler = handlers.find(d.job().getHandlerType())
                    .orElseThrow(() -> new IllegalStateException(
                            "handler missing at dispatch: " + d.job().getHandlerType()));
            WorkerTask task = new WorkerTask(d.job(), handler, d.workerId(), d.attempt(), d.epoch(),
                    props.leaseDuration().toMillis(),
                    !Boolean.parseBoolean(d.job().getAttributes()
                            .getOrDefault("dispatch.disable-heartbeat", "false")),
                    this::heartbeat, this::complete,
                    this::isJobCancelled, heartbeatExecutor);
            workers.execute(task);
        }
    }

    /** 扫描并处理过期租约（后台轮询；测试也可直接调用）。 */
    void sweepExpiredLeases() {
        boolean changed = false;
        synchronized (scheduleLock) {
            long now = clock.nowMillis();
            for (JobRecord job : store.findAll()) {
                if (job.getStatus() == JobStatus.LEASED && job.getLeaseExpiresAt() <= now) {
                    String owner = job.getLeaseOwner();
                    long epoch = job.getLeaseEpoch();
                    log.warn("租约过期 jobId={} owner={} epoch={} attempt={} 到期时间={} <= 当前={}：执行方判定失联",
                            job.getJobId(), owner, epoch, job.getAttempts(),
                            job.getLeaseExpiresAt(), now);
                    // 失联按一次可重试失败计数：耗尽则 EXHAUSTED，否则退避后可被重新分配，不永久卡住
                    scheduleRetryOrExhaust(job, FailureType.LEASE_EXPIRED,
                            "执行方租约过期（失联）：owner=%s".formatted(owner), now,
                            "租约过期回收：owner=%s epoch=%d，按可重试失败计数".formatted(owner, epoch));
                    changed = true;
                }
            }
        }
        if (changed) {
            safeDispatchLoop();
        }
    }

    // ---- 内部工具 ----

    private void scheduleRetryOrExhaust(JobRecord job, FailureType type, String message,
                                        long now, String reason) {
        int attempt = job.getAttempts();
        if (attempt >= retryPolicy.maxAttempts()) {
            // 终态统一为 EXHAUSTED；失败分类区分“普通重试耗尽”与“租约过期耗尽”
            FailureType exhaustedType = type == FailureType.LEASE_EXPIRED
                    ? FailureType.LEASE_EXPIRED : FailureType.RETRY_EXHAUSTED;
            job.markExhausted(exhaustedType,
                    (type == FailureType.LEASE_EXPIRED ? "租约过期重试耗尽：" : "重试次数耗尽：") + message,
                    now, reason + "；已达最大尝试次数%d，进入 EXHAUSTED 终态"
                            .formatted(retryPolicy.maxAttempts()));
            logTransition(job, "重试耗尽");
        } else {
            long backoff = retryPolicy.backoffAfterAttempt(attempt).toMillis();
            job.markWaitingRetry(type, message, now + backoff, now,
                    reason + "；第%d/%d次尝试，退避%dms后重新可调度"
                            .formatted(attempt, retryPolicy.maxAttempts(), backoff));
            logTransition(job, "退避等待重试");
        }
    }

    private boolean isJobCancelled(String jobId) {
        return store.findByJobId(jobId)
                .map(j -> j.getStatus() == JobStatus.CANCELLED)
                .orElse(true);
    }

    private void safeDispatchLoop() {
        try {
            if (running) {
                dispatchLoop();
            }
        } catch (Throwable t) {
            // 调度异常不静默吞掉，也不让后台线程因异常终止
            log.error("调度扫描发生异常，已记录并等待下一轮", t);
        }
    }

    private void safeSweep() {
        try {
            if (running) {
                sweepExpiredLeases();
            }
        } catch (Throwable t) {
            log.error("租约扫描发生异常，已记录并等待下一轮", t);
        }
    }

    private int countLeases() {
        int count = 0;
        for (JobRecord job : store.findAll()) {
            if (job.getStatus() == JobStatus.LEASED) {
                count++;
            }
        }
        return count;
    }

    private int countWaiting() {
        int count = 0;
        for (JobRecord job : store.findAll()) {
            JobStatus s = job.getStatus();
            if (s == JobStatus.QUEUED || s == JobStatus.WAITING_RETRY) {
                count++;
            }
        }
        return count;
    }

    /**
     * 选取下一个可调度作业：
     * 仅 QUEUED 与已到退避时刻的 WAITING_RETRY 可参与；排序为优先级降序、提交序号升序（FIFO 同优先级）。
     */
    private JobRecord pickNext(long now) {
        return store.findAll().stream()
                .filter(j -> j.getStatus() == JobStatus.QUEUED
                        || (j.getStatus() == JobStatus.WAITING_RETRY && j.getReadyAt() <= now))
                .min(Comparator.comparingInt(JobRecord::getPriority).reversed()
                        .thenComparingLong(JobRecord::getSubmissionSeq))
                .orElse(null);
    }

    private int queuePositionOf(JobRecord target) {
        long now = clock.nowMillis();
        List<JobRecord> ordered = store.findAll().stream()
                .filter(j -> j.getStatus() == JobStatus.QUEUED
                        || (j.getStatus() == JobStatus.WAITING_RETRY && j.getReadyAt() <= now))
                .sorted(Comparator.comparingInt(JobRecord::getPriority).reversed()
                        .thenComparingLong(JobRecord::getSubmissionSeq))
                .toList();
        return ordered.indexOf(target) + 1;
    }

    private void logTransition(JobRecord job, String decision) {
        var events = job.getEvents();
        var last = events.get(events.size() - 1);
        log.info("jobId={} 状态流转 {} -> {} 判定依据=[{}] 事件={} 尝试次数={}/{} failureType={}",
                job.getJobId(),
                last.from() == null ? "-" : last.from(),
                last.to(),
                decision + "；" + last.reason(),
                events.size(),
                job.getAttempts(),
                retryPolicy.maxAttempts(),
                job.getFailureType());
    }

    private record Dispatched(JobRecord job, String workerId, int attempt, long epoch) {
    }
}
