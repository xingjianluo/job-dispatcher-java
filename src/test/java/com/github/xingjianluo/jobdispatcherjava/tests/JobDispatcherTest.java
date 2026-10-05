package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.job.AcquireResult;
import com.github.xingjianluo.jobdispatcherjava.job.CancelOutcome;
import com.github.xingjianluo.jobdispatcherjava.job.CancelResult;
import com.github.xingjianluo.jobdispatcherjava.job.FailureKind;
import com.github.xingjianluo.jobdispatcherjava.job.Job;
import com.github.xingjianluo.jobdispatcherjava.job.JobConflictException;
import com.github.xingjianluo.jobdispatcherjava.job.JobDispatcher;
import com.github.xingjianluo.jobdispatcherjava.job.JobDispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.job.JobStatus;
import com.github.xingjianluo.jobdispatcherjava.job.JobTransition;
import com.github.xingjianluo.jobdispatcherjava.job.SubmitOutcome;
import com.github.xingjianluo.jobdispatcherjava.job.SubmitResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调度器语义测试：并发抢占、租约过期、失败分类与有界重试。
 * 每个用例打印作业标识、状态流转与判定依据。
 */
class JobDispatcherTest {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcherTest.class);

    /** 可手动推进的时钟，用于确定性模拟租约过期与退避到期。 */
    static class MutableClock extends Clock {
        private volatile Instant now = Instant.parse("2026-09-24T00:00:00Z");

        @Override
        public ZoneId getZone() { return ZoneId.of("UTC"); }

        @Override
        public Clock withZone(ZoneId zone) { return this; }

        @Override
        public Instant instant() { return now; }

        void advance(Duration duration) {
            now = now.plus(duration);
            log.info("[clock] advanced by {} -> now={}", duration, now);
        }
    }

    private MutableClock clock;
    private JobDispatcherProperties properties;
    private JobDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        properties = new JobDispatcherProperties();
        properties.setMaxConcurrent(2);
        properties.setMaxQueueCapacity(10);
        properties.setLeaseDuration(Duration.ofSeconds(30));
        properties.setMaxAttempts(3);
        properties.setRetryBackoffBase(Duration.ofMillis(100));
        dispatcher = new JobDispatcher(properties, clock);
    }

    private void logJob(String checkpoint, Job job, String rationale) {
        log.info("[assert] checkpoint={} jobId={} status={} attempt={} failureKind={} rationale={}",
                checkpoint, job.getId(), job.getStatus(), job.getAttempt(), job.getFailureKind(), rationale);
    }

    @Test
    void duplicateSubmitWithSameIdempotencyKeyDoesNotCreateSecondJob() {
        SubmitResult first = dispatcher.submit("key-1", 5, "payload");
        SubmitResult second = dispatcher.submit("key-1", 5, "payload");

        assertEquals(SubmitOutcome.ACCEPTED, first.outcome());
        assertEquals(SubmitOutcome.DUPLICATE, second.outcome());
        assertEquals(first.job().getId(), second.job().getId());
        assertEquals(1, dispatcher.listJobs().size());
        logJob("idempotent-submit", first.job(),
                "duplicate key returns existing job, no second execution will be scheduled");
    }

    @Test
    void higherPriorityJobIsScheduledFirstAndCapacityBlocksOthers() {
        properties.setMaxConcurrent(1);
        dispatcher = new JobDispatcher(properties, clock);

        Job low = dispatcher.submit("low", 1, "p").job();
        Job high = dispatcher.submit("high", 9, "p").job();

        AcquireResult first = dispatcher.acquire("worker-1");
        assertTrue(first.acquired());
        assertEquals(high.getId(), first.job().getId());
        logJob("priority-order", high, "higher priority dequeued before lower priority");

        AcquireResult second = dispatcher.acquire("worker-2");
        assertFalse(second.acquired());
        log.info("[assert] checkpoint=capacity-blocked jobId={} status={} rationale={}",
                low.getId(), low.getStatus(),
                "concurrency capacity reached, job stays queued instead of being dropped or over-issued: " + second.reason());
        assertEquals(JobStatus.QUEUED, low.getStatus());
    }

    @Test
    void submitIsRejectedWithReasonWhenQueueIsFull() {
        properties.setMaxQueueCapacity(2);
        dispatcher = new JobDispatcher(properties, clock);

        dispatcher.submit("k1", 1, "p");
        dispatcher.submit("k2", 1, "p");
        SubmitResult third = dispatcher.submit("k3", 1, "p");

        assertEquals(SubmitOutcome.REJECTED_QUEUE_FULL, third.outcome());
        log.info("[assert] checkpoint=queue-full outcome={} rationale={}", third.outcome(), third.reason());
    }

    @Test
    void concurrentAcquireGrantsLeaseToExactlyOneOwner() throws Exception {
        Job job = dispatcher.submit("race", 5, "p").job();
        int contenders = 8;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<AcquireResult>> futures = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String owner = "worker-" + i;
            futures.add(pool.submit(() -> {
                start.await();
                return dispatcher.acquire(owner);
            }));
        }
        start.countDown();
        List<AcquireResult> results = new ArrayList<>();
        for (Future<AcquireResult> f : futures) {
            results.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();

        List<AcquireResult> winners = results.stream().filter(AcquireResult::acquired).toList();
        assertEquals(1, winners.size());
        String winnerOwner = winners.get(0).job().getLeaseOwner();
        String winnerLease = winners.get(0).leaseId();
        logJob("single-winner", job,
                "exactly one of " + contenders + " contenders holds the lease: owner=" + winnerOwner);

        // 失败方的无效租约不得完成作业，防止并发抢占造成重复生效
        assertThrows(JobConflictException.class,
                () -> dispatcher.complete(job.getId(), "stale-lease", "worker-loser"));
        dispatcher.complete(job.getId(), winnerLease, winnerOwner);
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        logJob("completed-by-winner", job, "only the valid lease holder can complete the job");
    }

    @Test
    void expiredLeaseIsReclaimedAndStaleOwnerCannotComplete() {
        properties.setLeaseDuration(Duration.ofSeconds(10));
        dispatcher = new JobDispatcher(properties, clock);

        Job job = dispatcher.submit("lease-expiry", 5, "p").job();
        AcquireResult first = dispatcher.acquire("owner-A");
        assertTrue(first.acquired());
        logJob("leased-to-A", job, "owner-A holds lease expiring at " + job.getLeaseExpiresAt());

        // owner-A 失联，租约到期
        clock.advance(Duration.ofSeconds(11));
        int reaped = dispatcher.reapExpiredLeases();
        assertEquals(1, reaped);
        assertEquals(JobStatus.QUEUED, job.getStatus());
        logJob("reclaimed", job, "expired lease reclaimed, job re-queued with backoff instead of stuck forever");

        // 回收后按退避安排下一次可被抢占的时间，退避到期前不会被立刻抢走
        assertFalse(dispatcher.acquire("owner-early").acquired());
        clock.advance(Duration.ofMillis(150));

        // owner-B 重新获取；owner-A 的过期租约不得再生效（不重复执行）
        AcquireResult second = dispatcher.acquire("owner-B");
        assertTrue(second.acquired());
        assertThrows(JobConflictException.class,
                () -> dispatcher.complete(job.getId(), first.leaseId(), "owner-A"));
        log.info("[assert] checkpoint=stale-owner-rejected jobId={} rationale={}",
                job.getId(), "stale lease from owner-A rejected with conflict, no duplicate execution");

        dispatcher.complete(job.getId(), second.leaseId(), "owner-B");
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        logJob("completed-once", job, "job completed exactly once by the current lease holder");
    }

    @Test
    void renewExtendsLeaseAndPreventsReclaim() {
        properties.setLeaseDuration(Duration.ofSeconds(10));
        dispatcher = new JobDispatcher(properties, clock);

        Job job = dispatcher.submit("renew", 5, "p").job();
        AcquireResult acquired = dispatcher.acquire("owner-A");

        clock.advance(Duration.ofSeconds(8));
        assertTrue(dispatcher.renew(job.getId(), acquired.leaseId(), "owner-A"));
        clock.advance(Duration.ofSeconds(8)); // 总计 16s，但续期后未到期
        assertEquals(0, dispatcher.reapExpiredLeases());
        assertEquals(JobStatus.LEASED, job.getStatus());
        logJob("renew-keeps-lease", job, "renewed lease survives past original expiry");

        assertThrows(JobConflictException.class,
                () -> dispatcher.renew(job.getId(), "wrong-lease", "owner-A"));
    }

    @Test
    void retryableFailureRetriesWithBackoffAndEventuallySucceeds() {
        Job job = dispatcher.submit("retry", 5, "p").job();

        AcquireResult first = dispatcher.acquire("w1");
        dispatcher.fail(job.getId(), first.leaseId(), "w1", true, "transient error");
        assertEquals(JobStatus.QUEUED, job.getStatus());
        logJob("backoff-pending", job, "retryable failure re-queued with backoff, not yet due");

        // 退避未到期，不可被获取
        AcquireResult tooEarly = dispatcher.acquire("w2");
        assertFalse(tooEarly.acquired());
        log.info("[assert] checkpoint=backoff-not-due jobId={} rationale={}", job.getId(), tooEarly.reason());

        clock.advance(Duration.ofMillis(150));
        AcquireResult second = dispatcher.acquire("w2");
        assertTrue(second.acquired());
        dispatcher.complete(job.getId(), second.leaseId(), "w2");
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        assertEquals(2, job.getAttempt());
        logJob("retry-succeeded", job, "retry executed after backoff and succeeded on attempt 2");
    }

    @Test
    void nonRetryableFailureGoesToDistinguishableTerminalState() {
        Job job = dispatcher.submit("fatal", 5, "p").job();
        AcquireResult acquired = dispatcher.acquire("w1");
        dispatcher.fail(job.getId(), acquired.leaseId(), "w1", false, "bad payload");

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals(FailureKind.NON_RETRYABLE, job.getFailureKind());
        assertEquals("bad payload", job.getFailureMessage());
        logJob("non-retryable-terminal", job, "non-retryable failure is terminal with kind NON_RETRYABLE");
    }

    @Test
    void retriesExhaustedGoesToDistinguishableTerminalState() {
        properties.setMaxAttempts(2);
        dispatcher = new JobDispatcher(properties, clock);
        Job job = dispatcher.submit("exhaust", 5, "p").job();

        AcquireResult a1 = dispatcher.acquire("w1");
        dispatcher.fail(job.getId(), a1.leaseId(), "w1", true, "boom-1");
        clock.advance(Duration.ofMillis(150));

        AcquireResult a2 = dispatcher.acquire("w1");
        assertTrue(a2.acquired());
        dispatcher.fail(job.getId(), a2.leaseId(), "w1", true, "boom-2");

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals(FailureKind.RETRIES_EXHAUSTED, job.getFailureKind());
        assertEquals(2, job.getAttempt());
        logJob("retries-exhausted", job,
                "bounded retries exhausted at attempt 2/2, terminal kind RETRIES_EXHAUSTED (distinct from NON_RETRYABLE)");
    }

    @Test
    void cancelIsEffectiveOnceAndRepeatCancelIsDetectable() {
        Job job = dispatcher.submit("cancel-me", 5, "p").job();

        assertTrue(dispatcher.cancel(job.getId()));
        assertEquals(JobStatus.CANCELLED, job.getStatus());
        assertEquals(FailureKind.CANCELLED, job.getFailureKind());
        logJob("cancelled", job, "cancel effective, terminal with kind CANCELLED");

        assertFalse(dispatcher.cancel(job.getId()));
        log.info("[assert] checkpoint=repeat-cancel jobId={} status={} rationale={}",
                job.getId(), job.getStatus(), "second cancel returns false: already terminal, distinguishable");

        // 已取消作业不得再被调度执行
        assertFalse(dispatcher.acquire("w1").acquired());
    }

    @Test
    void cancellingLeasedJobInvalidatesTheLease() {
        Job job = dispatcher.submit("cancel-leased", 5, "p").job();
        AcquireResult acquired = dispatcher.acquire("w1");
        assertTrue(acquired.acquired());

        assertTrue(dispatcher.cancel(job.getId()));
        assertThrows(JobConflictException.class,
                () -> dispatcher.complete(job.getId(), acquired.leaseId(), "w1"));
        logJob("cancel-invalidates-lease", job,
                "lease invalidated by cancel; holder cannot complete afterwards, no silent swallow");
    }

    @Test
    void concurrentContentionNeverExecutesJobTwice() throws Exception {
        // 多作业 + 多执行方 + 租约回收并发进行，统计“成功完成”必须恰好每作业一次
        properties.setMaxConcurrent(4);
        properties.setMaxQueueCapacity(100);
        properties.setLeaseDuration(Duration.ofMillis(50));
        dispatcher = new JobDispatcher(properties, clock);

        int jobCount = 20;
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobIds.add(dispatcher.submit("bulk-" + i, i % 5, "p").job().getId());
        }

        AtomicInteger completions = new AtomicInteger();
        CopyOnWriteArrayList<String> completionLog = new CopyOnWriteArrayList<>();
        int workers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String owner = "worker-" + w;
            futures.add(pool.submit(() -> {
                start.await();
                int idleRounds = 0;
                while (idleRounds < 20) {
                    dispatcher.reapExpiredLeases();
                    AcquireResult result = dispatcher.acquire(owner);
                    if (!result.acquired()) {
                        idleRounds++;
                        Thread.sleep(5);
                        continue;
                    }
                    idleRounds = 0;
                    Job job = result.job();
                    try {
                        dispatcher.complete(job.getId(), result.leaseId(), owner);
                        completions.incrementAndGet();
                        completionLog.add(job.getId() + " by " + owner);
                        log.info("[worker] jobId={} completed by owner={}", job.getId(), owner);
                    } catch (JobConflictException conflict) {
                        // 租约被回收后作业被他人完成：冲突被显式抛出而非静默吞掉
                        log.info("[worker] jobId={} owner={} lost lease: {}", job.getId(), owner, conflict.getMessage());
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[assert] checkpoint=no-duplicate-completion completions={} jobCount={} log={}",
                completions.get(), jobCount, completionLog);
        assertEquals(jobCount, completions.get(), "each job must be completed exactly once");
        assertEquals(jobCount, completionLog.stream().distinct().count());
        for (String jobId : jobIds) {
            Job job = dispatcher.find(jobId).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, job.getStatus());
            assertNotNull(job.getCreatedAt());
        }
    }

    @Test
    void reapConsumesAttemptAndBackoffDelaysReacquire() {
        properties.setLeaseDuration(Duration.ofSeconds(10));
        dispatcher = new JobDispatcher(properties, clock);

        Job job = dispatcher.submit("lost-once", 5, "p").job();
        AcquireResult first = dispatcher.acquire("owner-A");
        assertTrue(first.acquired());
        assertEquals(1, job.getAttempt());

        // owner-A 失联，租约到期被回收：这次租约已消耗一次尝试机会
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, dispatcher.reapExpiredLeases());
        assertEquals(JobStatus.QUEUED, job.getStatus());
        assertEquals(1, job.getAttempt());
        logJob("reap-consumes-attempt", job,
                "reclaim consumes the attempt spent on the lost lease, backoff scheduled at "
                        + job.getNextAttemptAt());

        // 退避未到期：不能刚回收就立刻被别人抢走空转
        AcquireResult tooEarly = dispatcher.acquire("owner-B");
        assertFalse(tooEarly.acquired());
        log.info("[assert] checkpoint=reap-backoff-not-due jobId={} rationale={}",
                job.getId(), "reclaimed job not stealable before backoff elapses: " + tooEarly.reason());

        // 退避到期后才能被重新抢占；失联执行方的迟到回执不得再生效
        clock.advance(Duration.ofMillis(150));
        AcquireResult second = dispatcher.acquire("owner-B");
        assertTrue(second.acquired());
        assertEquals(2, job.getAttempt());
        assertThrows(JobConflictException.class,
                () -> dispatcher.complete(job.getId(), first.leaseId(), "owner-A"));
        log.info("[assert] checkpoint=stale-lost-owner-rejected jobId={} rationale={}",
                job.getId(), "late ack from lost owner-A rejected, job not executed twice");

        dispatcher.complete(job.getId(), second.leaseId(), "owner-B");
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        logJob("recovered-after-loss", job, "job completed exactly once after owner loss and backoff");
    }

    @Test
    void repeatedOwnerLossEndsInDistinguishableLostTerminalState() {
        properties.setMaxAttempts(2);
        properties.setLeaseDuration(Duration.ofSeconds(10));
        dispatcher = new JobDispatcher(properties, clock);

        Job job = dispatcher.submit("always-lost", 5, "p").job();

        // 第 1 次失联：回收后仍可重试
        dispatcher.acquire("owner-A");
        clock.advance(Duration.ofSeconds(11));
        dispatcher.reapExpiredLeases();
        assertEquals(JobStatus.QUEUED, job.getStatus());
        logJob("lost-attempt-1", job, "first loss reclaimed into bounded retry, not terminal yet");

        // 第 2 次失联：尝试次数耗尽，进入终态 FAILED / LOST
        clock.advance(Duration.ofMillis(150));
        AcquireResult second = dispatcher.acquire("owner-B");
        assertTrue(second.acquired());
        clock.advance(Duration.ofSeconds(11));
        dispatcher.reapExpiredLeases();

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals(FailureKind.LOST, job.getFailureKind());
        assertEquals(2, job.getAttempt());
        logJob("lost-terminal", job,
                "repeated owner loss exhausted attempts, terminal kind LOST "
                        + "(distinct from RETRIES_EXHAUSTED and NON_RETRYABLE)");

        // 不会无限循环：终态后回收与调度都不再触碰它
        assertEquals(0, dispatcher.reapExpiredLeases());
        assertFalse(dispatcher.acquire("owner-C").acquired());
        log.info("[assert] checkpoint=lost-no-recycle jobId={} status={} rationale={}",
                job.getId(), job.getStatus(),
                "terminal LOST job is never re-queued or re-leased, no infinite loop, no stuck lease");
    }

    @Test
    void cancelDistinguishesEffectiveAlreadyCancelledAndAlreadyTerminal() {
        // 1) 本次确实取消
        Job cancelled = dispatcher.submit("cancel-fresh", 5, "p").job();
        CancelResult first = dispatcher.cancelJob(cancelled.getId());
        assertEquals(CancelOutcome.CANCELLED, first.outcome());
        assertTrue(first.effective());
        assertEquals(JobStatus.CANCELLED, cancelled.getStatus());
        logJob("cancel-effective", cancelled, "cancel outcome=CANCELLED, took effect this time");

        // 2) 之前已经被取消过
        CancelResult repeat = dispatcher.cancelJob(cancelled.getId());
        assertEquals(CancelOutcome.ALREADY_CANCELLED, repeat.outcome());
        assertFalse(repeat.effective());
        assertEquals(JobStatus.CANCELLED, cancelled.getStatus());
        assertEquals(FailureKind.CANCELLED, cancelled.getFailureKind());
        log.info("[assert] checkpoint=cancel-repeat jobId={} outcome={} rationale={}",
                cancelled.getId(), repeat.outcome(), repeat.reason());

        // 3) 已因其它原因进入终态：状态与失败原因保持不变
        Job succeeded = dispatcher.submit("cancel-success", 5, "p").job();
        AcquireResult acquired = dispatcher.acquire("w1");
        dispatcher.complete(succeeded.getId(), acquired.leaseId(), "w1");
        CancelResult afterSuccess = dispatcher.cancelJob(succeeded.getId());
        assertEquals(CancelOutcome.ALREADY_TERMINAL, afterSuccess.outcome());
        assertEquals(JobStatus.SUCCEEDED, succeeded.getStatus());
        assertEquals(null, succeeded.getFailureKind());
        log.info("[assert] checkpoint=cancel-after-success jobId={} outcome={} rationale={}",
                succeeded.getId(), afterSuccess.outcome(), afterSuccess.reason());

        Job failed = dispatcher.submit("cancel-failed", 5, "p").job();
        AcquireResult acquired2 = dispatcher.acquire("w1");
        dispatcher.fail(failed.getId(), acquired2.leaseId(), "w1", false, "bad payload");
        CancelResult afterFail = dispatcher.cancelJob(failed.getId());
        assertEquals(CancelOutcome.ALREADY_TERMINAL, afterFail.outcome());
        assertEquals(JobStatus.FAILED, failed.getStatus());
        assertEquals(FailureKind.NON_RETRYABLE, failed.getFailureKind());
        assertEquals("bad payload", failed.getFailureMessage());
        log.info("[assert] checkpoint=cancel-after-failure jobId={} outcome={} rationale={}",
                failed.getId(), afterFail.outcome(),
                "original terminal state and failure reason untouched: " + afterFail.reason());

        // 旧 boolean 接口语义保持不变
        assertTrue(dispatcher.cancel(dispatcher.submit("cancel-legacy", 5, "p").job().getId()));
        assertFalse(dispatcher.cancel(cancelled.getId()));
    }

    @Test
    void transitionHistoryCoversLifecycleInOrder() {
        properties.setLeaseDuration(Duration.ofSeconds(10));
        dispatcher = new JobDispatcher(properties, clock);

        Job job = dispatcher.submit("history", 5, "p").job();
        AcquireResult acquired = dispatcher.acquire("owner-A");
        dispatcher.renew(job.getId(), acquired.leaseId(), "owner-A");
        dispatcher.fail(job.getId(), acquired.leaseId(), "owner-A", true, "transient");
        clock.advance(Duration.ofMillis(150));
        AcquireResult second = dispatcher.acquire("owner-B");
        dispatcher.complete(job.getId(), second.leaseId(), "owner-B");

        List<JobTransition> history = dispatcher.history(job.getId());
        for (JobTransition t : history) {
            log.info("[history] jobId={} seq={} {} -> {} version={} at={} rationale={}",
                    job.getId(), t.sequence(), t.from(), t.to(), t.version(), t.at(), t.rationale());
        }
        // 入队、被谁拿走、续期、触发重试、再次被拿走、终态，每条都有判定依据
        assertEquals(6, history.size());
        assertEquals(null, history.get(0).from());
        assertEquals(JobStatus.QUEUED, history.get(0).to());   // 入队
        assertEquals(JobStatus.LEASED, history.get(1).to());   // 被 owner-A 拿走
        assertEquals(JobStatus.LEASED, history.get(2).to());   // 续期记录
        assertEquals(JobStatus.QUEUED, history.get(3).to());   // 退避重试
        assertEquals(JobStatus.LEASED, history.get(4).to());   // 退避到期后被 owner-B 拿走
        assertEquals(JobStatus.SUCCEEDED, history.get(5).to()); // 终态
        for (int i = 0; i < history.size(); i++) {
            assertEquals(i, history.get(i).version(), "versions must be consecutive from 0, no loss no dup");
            assertFalse(history.get(i).rationale().isBlank(), "every entry carries its rationale");
            if (i > 0) {
                assertTrue(history.get(i).sequence() > history.get(i - 1).sequence(),
                        "sequence strictly increasing");
                assertEquals(history.get(i - 1).to(), history.get(i).from(),
                        "chain of states must be continuous");
            }
        }
        assertEquals(history.get(history.size() - 1).to(), job.getStatus());
        log.info("[assert] checkpoint=history-ordered jobId={} entries={} rationale={}",
                job.getId(), history.size(),
                "lifecycle recorded in order, continuous chain, consistent with final state");
    }

    @Test
    void transitionHistoryIsCompleteAndOrderedUnderConcurrency() throws Exception {
        // 并发抢占 + 退避重试 + 时钟推进同时进行，校验流转记录不丢不重且与最终状态一致
        properties.setMaxConcurrent(4);
        properties.setMaxQueueCapacity(100);
        properties.setLeaseDuration(Duration.ofSeconds(30));
        properties.setRetryBackoffBase(Duration.ofMillis(20));
        dispatcher = new JobDispatcher(properties, clock);

        int jobCount = 20;
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobIds.add(dispatcher.submit("hist-" + i, i % 5, "p").job().getId());
        }

        int workers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(workers + 1);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String owner = "worker-" + w;
            futures.add(pool.submit(() -> {
                start.await();
                int idleRounds = 0;
                while (idleRounds < 50) {
                    AcquireResult result = dispatcher.acquire(owner);
                    if (!result.acquired()) {
                        idleRounds++;
                        Thread.sleep(2);
                        continue;
                    }
                    idleRounds = 0;
                    Job job = result.job();
                    if (job.getAttempt() < 2) {
                        // 第一次尝试必失败（可重试），制造并发下的退避重试流转
                        dispatcher.fail(job.getId(), result.leaseId(), owner, true, "transient-by-test");
                    } else {
                        dispatcher.complete(job.getId(), result.leaseId(), owner);
                    }
                }
                return null;
            }));
        }
        // 时钟推进线程：让退避到期，驱动重试流转
        futures.add(pool.submit(() -> {
            start.await();
            for (int i = 0; i < 400; i++) {
                clock.advance(Duration.ofMillis(10));
                Thread.sleep(2);
            }
            return null;
        }));
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        Set<Long> allSequences = new HashSet<>();
        int totalEntries = 0;
        for (String jobId : jobIds) {
            Job job = dispatcher.find(jobId).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, job.getStatus());
            List<JobTransition> history = dispatcher.history(jobId);
            totalEntries += history.size();
            // 不丢不重：版本号从 0 连续递增，状态链连续，结尾与最终状态一致
            for (int i = 0; i < history.size(); i++) {
                JobTransition t = history.get(i);
                assertEquals(i, t.version(),
                        "job " + jobId + " history version gap/dup at index " + i);
                assertTrue(allSequences.add(t.sequence()),
                        "duplicate global sequence " + t.sequence());
                if (i > 0) {
                    assertEquals(history.get(i - 1).to(), t.from(),
                            "job " + jobId + " broken state chain at index " + i);
                }
            }
            assertEquals(job.getStatus(), history.get(history.size() - 1).to(),
                    "job " + jobId + " last history entry must match final state");
        }
        log.info("[assert] checkpoint=history-concurrent-consistent jobs={} totalEntries={} rationale={}",
                jobCount, totalEntries,
                "no lost/duplicated transition records under concurrency; "
                        + "per-job versions consecutive, sequences globally unique, order matches final state");
        assertEquals(jobCount * 5, totalEntries,
                "each job: enqueue + lease + retry-fail-requeue + lease + complete = 5 entries");
    }
}
