package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.job.AcquireResult;
import com.github.xingjianluo.jobdispatcherjava.job.CancelOutcome;
import com.github.xingjianluo.jobdispatcherjava.job.CancelResult;
import com.github.xingjianluo.jobdispatcherjava.job.FailureKind;
import com.github.xingjianluo.jobdispatcherjava.job.Job;
import com.github.xingjianluo.jobdispatcherjava.job.JobConflictException;
import com.github.xingjianluo.jobdispatcherjava.job.JobDispatcher;
import com.github.xingjianluo.jobdispatcherjava.job.JobDispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.job.JobEvent;
import com.github.xingjianluo.jobdispatcherjava.job.JobStatus;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运维与排查能力测试：失联回收的有界重试、取消三态判定、状态流转记录。
 * 每个用例打印作业标识、状态流转与判定依据。
 */
class JobDispatcherOpsTest {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcherOpsTest.class);

    /** 可手动推进的时钟，用于确定性模拟租约过期与退避到期。 */
    static class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-05T00:00:00Z");

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
        properties.setLeaseDuration(Duration.ofSeconds(10));
        properties.setMaxAttempts(3);
        properties.setRetryBackoffBase(Duration.ofMillis(100));
        dispatcher = new JobDispatcher(properties, clock);
    }

    private void logHistory(String checkpoint, String jobId) {
        List<JobEvent> events = dispatcher.history(jobId);
        for (JobEvent e : events) {
            log.info("[history] checkpoint={} seq={} jobId={} {} -> {} attempt={} actor={} rationale={}",
                    checkpoint, e.sequence(), e.jobId(), e.fromStatus(), e.toStatus(),
                    e.attempt(), e.actor(), e.rationale());
        }
    }

    @Test
    void reclaimConsumesAttemptAndBackoffGatesNextAcquire() {
        Job job = dispatcher.submit("lost-1", 5, "p").job();
        AcquireResult first = dispatcher.acquire("owner-A");
        assertTrue(first.acquired());
        assertEquals(1, job.getAttempt());

        // owner-A 失联，租约到期被回收：本次尝试已被消耗
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, dispatcher.reapExpiredLeases());
        assertEquals(JobStatus.QUEUED, job.getStatus());
        assertEquals(1, job.getAttempt());
        assertEquals(clock.instant().plus(Duration.ofMillis(100)), job.getNextAttemptAt());
        log.info("[assert] checkpoint=reclaim-consumes-attempt jobId={} attempt={}/{} nextAttemptAt={} rationale={}",
                job.getId(), job.getAttempt(), job.getMaxAttempts(), job.getNextAttemptAt(),
                "reclaim keeps the consumed attempt and schedules next attempt after backoff");

        // 退避窗口内不可被抢占，不会刚回收就空转
        AcquireResult tooEarly = dispatcher.acquire("owner-B");
        assertFalse(tooEarly.acquired());
        log.info("[assert] checkpoint=backoff-gating jobId={} rationale={}", job.getId(), tooEarly.reason());

        // 退避到期后才能被重新抢占，且消耗下一次尝试
        clock.advance(Duration.ofMillis(100));
        AcquireResult second = dispatcher.acquire("owner-B");
        assertTrue(second.acquired());
        assertEquals(2, job.getAttempt());
        log.info("[assert] checkpoint=reacquired-after-backoff jobId={} attempt={} owner={} rationale={}",
                job.getId(), job.getAttempt(), job.getLeaseOwner(),
                "acquirable only after backoff elapsed, attempt incremented to 2");
    }

    @Test
    void repeatedOwnerLossEndsInDistinguishableTerminalState() {
        properties.setMaxAttempts(2);
        dispatcher = new JobDispatcher(properties, clock);
        Job job = dispatcher.submit("lost-forever", 5, "p").job();

        // 第 1 次失联：回收后退避重入队
        AcquireResult a1 = dispatcher.acquire("owner-A");
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, dispatcher.reapExpiredLeases());
        assertEquals(JobStatus.QUEUED, job.getStatus());

        // 第 2 次失联：尝试次数耗尽，进入失联专属终态
        clock.advance(Duration.ofMillis(150));
        AcquireResult a2 = dispatcher.acquire("owner-B");
        assertTrue(a2.acquired());
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, dispatcher.reapExpiredLeases());

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals(FailureKind.LOST_RETRIES_EXHAUSTED, job.getFailureKind());
        assertTrue(job.getFailureMessage().contains("owner-B"));
        log.info("[assert] checkpoint=lost-terminal jobId={} status={} failureKind={} attempt={}/{} rationale={}",
                job.getId(), job.getStatus(), job.getFailureKind(), job.getAttempt(), job.getMaxAttempts(),
                "repeated owner loss exhausts attempts: terminal LOST_RETRIES_EXHAUSTED, "
                        + "distinct from RETRIES_EXHAUSTED and NON_RETRYABLE");
        assertTrue(job.getFailureKind() != FailureKind.RETRIES_EXHAUSTED
                && job.getFailureKind() != FailureKind.NON_RETRYABLE);

        // 终态后：不再被回收、不再被抢占，不会无限循环或卡死
        clock.advance(Duration.ofSeconds(60));
        assertEquals(0, dispatcher.reapExpiredLeases());
        assertFalse(dispatcher.acquire("owner-C").acquired());
        assertEquals(JobStatus.FAILED, job.getStatus());

        // 迟到的旧执行方回执不得生效，不会造成重复执行
        assertThrows(JobConflictException.class,
                () -> dispatcher.complete(job.getId(), a2.leaseId(), "owner-B"));
        assertThrows(JobConflictException.class,
                () -> dispatcher.fail(job.getId(), a1.leaseId(), "owner-A", true, "late failure"));
        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals(FailureKind.LOST_RETRIES_EXHAUSTED, job.getFailureKind());
        logHistory("lost-terminal", job.getId());
    }

    @Test
    void lostRetriesExhaustedDoesNotConsumeExtraCapacity() {
        // 失联作业退避期间不占执行容量，其它作业可正常被调度；回收不超发
        properties.setMaxConcurrent(1);
        dispatcher = new JobDispatcher(properties, clock);
        Job lost = dispatcher.submit("lost-cap", 5, "p").job();
        Job other = dispatcher.submit("other-cap", 1, "p").job();

        AcquireResult a1 = dispatcher.acquire("owner-A");
        assertEquals(lost.getId(), a1.job().getId());
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, dispatcher.reapExpiredLeases());

        // lost 在退避中，other 可被获取（容量不被失联作业占用）
        AcquireResult a2 = dispatcher.acquire("owner-B");
        assertTrue(a2.acquired());
        assertEquals(other.getId(), a2.job().getId());
        // 执行容量已满，lost 即使退避到期也不会超发
        clock.advance(Duration.ofMillis(150));
        assertFalse(dispatcher.acquire("owner-C").acquired());
        log.info("[assert] checkpoint=no-over-issue jobId={} status={} rationale={}",
                lost.getId(), lost.getStatus(),
                "reclaimed job waits for both backoff and concurrency capacity, no over-issue");
    }

    @Test
    void cancelDistinguishesThreeOutcomes() {
        // 1) 本次取消生效
        Job queued = dispatcher.submit("cancel-effective", 5, "p").job();
        CancelResult r1 = dispatcher.cancelWithOutcome(queued.getId());
        assertEquals(CancelOutcome.CANCELLED, r1.outcome());
        assertTrue(r1.effective());
        assertEquals(JobStatus.CANCELLED, queued.getStatus());
        assertEquals(FailureKind.CANCELLED, queued.getFailureKind());
        log.info("[assert] checkpoint=cancel-effective jobId={} outcome={} rationale={}",
                queued.getId(), r1.outcome(), r1.reason());

        // 2) 此前已被取消过：状态与失败原因保持不变
        CancelResult r2 = dispatcher.cancelWithOutcome(queued.getId());
        assertEquals(CancelOutcome.ALREADY_CANCELLED, r2.outcome());
        assertFalse(r2.effective());
        assertEquals(JobStatus.CANCELLED, queued.getStatus());
        assertEquals(FailureKind.CANCELLED, queued.getFailureKind());
        log.info("[assert] checkpoint=already-cancelled jobId={} outcome={} rationale={}",
                queued.getId(), r2.outcome(), r2.reason());

        // 3a) 已成功：取消不生效，原状态不变
        Job succeeded = dispatcher.submit("cancel-succeeded", 5, "p").job();
        AcquireResult a = dispatcher.acquire("w1");
        dispatcher.complete(succeeded.getId(), a.leaseId(), "w1");
        CancelResult r3 = dispatcher.cancelWithOutcome(succeeded.getId());
        assertEquals(CancelOutcome.ALREADY_TERMINAL, r3.outcome());
        assertEquals(JobStatus.SUCCEEDED, succeeded.getStatus());
        assertNull(succeeded.getFailureKind());
        log.info("[assert] checkpoint=already-terminal-succeeded jobId={} outcome={} status={} rationale={}",
                succeeded.getId(), r3.outcome(), succeeded.getStatus(), r3.reason());

        // 3b) 已失败：取消不生效，失败原因不被覆盖
        Job failed = dispatcher.submit("cancel-failed", 5, "p").job();
        AcquireResult b = dispatcher.acquire("w1");
        dispatcher.fail(failed.getId(), b.leaseId(), "w1", false, "bad payload");
        CancelResult r4 = dispatcher.cancelWithOutcome(failed.getId());
        assertEquals(CancelOutcome.ALREADY_TERMINAL, r4.outcome());
        assertEquals(JobStatus.FAILED, failed.getStatus());
        assertEquals(FailureKind.NON_RETRYABLE, failed.getFailureKind());
        assertEquals("bad payload", failed.getFailureMessage());
        log.info("[assert] checkpoint=already-terminal-failed jobId={} outcome={} status={} failureKind={} rationale={}",
                failed.getId(), r4.outcome(), failed.getStatus(), failed.getFailureKind(), r4.reason());

        // 旧布尔接口语义保持不变
        Job legacy = dispatcher.submit("cancel-legacy", 5, "p").job();
        assertTrue(dispatcher.cancel(legacy.getId()));
        assertFalse(dispatcher.cancel(legacy.getId()));
        assertFalse(dispatcher.cancel(succeeded.getId()));
        log.info("[assert] checkpoint=legacy-boolean-compat rationale={}",
                "cancel(jobId) boolean API unchanged: true only when this call is effective");
    }

    @Test
    void historyRecordsFullLifecycleInOrder() {
        Job job = dispatcher.submit("history-1", 5, "p").job();
        AcquireResult a1 = dispatcher.acquire("owner-A");
        dispatcher.renew(job.getId(), a1.leaseId(), "owner-A");
        clock.advance(Duration.ofSeconds(11));
        dispatcher.reapExpiredLeases();
        clock.advance(Duration.ofMillis(150));
        AcquireResult a2 = dispatcher.acquire("owner-B");
        dispatcher.fail(job.getId(), a2.leaseId(), "owner-B", true, "transient");
        clock.advance(Duration.ofMillis(250));
        AcquireResult a3 = dispatcher.acquire("owner-C");
        dispatcher.complete(job.getId(), a3.leaseId(), "owner-C");

        List<JobEvent> events = dispatcher.history(job.getId());
        logHistory("full-lifecycle", job.getId());

        // 覆盖：提交入队、被谁拿走、续期、失联回收、退避重试、终态及原因
        assertEquals(JobStatus.QUEUED, events.get(0).toStatus());
        assertNull(events.get(0).fromStatus());
        assertTrue(events.stream().anyMatch(e -> e.toStatus() == JobStatus.LEASED
                && "owner-A".equals(e.actor())));
        assertTrue(events.stream().anyMatch(e -> e.fromStatus() == JobStatus.LEASED
                && e.toStatus() == JobStatus.LEASED && e.rationale().contains("renewed")));
        assertTrue(events.stream().anyMatch(e -> e.rationale().contains("reclaimed from lost owner=owner-A")));
        assertTrue(events.stream().anyMatch(e -> e.rationale().contains("retryable failure")));
        JobEvent last = events.get(events.size() - 1);
        assertEquals(JobStatus.SUCCEEDED, last.toStatus());
        assertTrue(last.rationale().contains("owner-C"));

        // 顺序与最终状态一致：链式衔接、序号严格递增
        assertHistoryChainConsistent(job, events);
    }

    @Test
    void concurrentTransitionsProduceCompleteOrderedHistory() throws Exception {
        properties.setMaxConcurrent(4);
        properties.setMaxQueueCapacity(100);
        properties.setLeaseDuration(Duration.ofMillis(30));
        dispatcher = new JobDispatcher(properties, clock);

        int jobCount = 20;
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobIds.add(dispatcher.submit("hist-bulk-" + i, i % 5, "p").job().getId());
        }

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
                        Thread.sleep(3);
                        continue;
                    }
                    idleRounds = 0;
                    Job job = result.job();
                    try {
                        dispatcher.renew(job.getId(), result.leaseId(), owner);
                        dispatcher.complete(job.getId(), result.leaseId(), owner);
                    } catch (JobConflictException conflict) {
                        log.info("[worker] jobId={} owner={} lost lease: {}",
                                job.getId(), owner, conflict.getMessage());
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

        // 不丢：每个作业都有完整记录且链式衔接到最终状态
        // 不重：全局序号无重复；每个作业内序号严格递增
        Set<Long> allSequences = new HashSet<>();
        int totalEvents = 0;
        for (String jobId : jobIds) {
            Job job = dispatcher.find(jobId).orElseThrow();
            List<JobEvent> events = dispatcher.history(jobId);
            assertFalse(events.isEmpty(), "history must not be lost for job " + jobId);
            assertHistoryChainConsistent(job, events);
            for (JobEvent e : events) {
                assertTrue(allSequences.add(e.sequence()),
                        "duplicate event sequence " + e.sequence() + " for job " + jobId);
            }
            totalEvents += events.size();
            assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        }
        assertEquals(totalEvents, allSequences.size());
        log.info("[assert] checkpoint=history-complete-ordered jobs={} totalEvents={} distinctSequences={} rationale={}",
                jobCount, totalEvents, allSequences.size(),
                "no lost or duplicated events under concurrency; per-job order matches final state");
    }

    /** 校验：首条为提交入队，相邻事件状态链式衔接，序号严格递增，末条与作业当前状态一致。 */
    private void assertHistoryChainConsistent(Job job, List<JobEvent> events) {
        assertNull(events.get(0).fromStatus());
        assertEquals(JobStatus.QUEUED, events.get(0).toStatus());
        for (int i = 1; i < events.size(); i++) {
            assertEquals(events.get(i - 1).toStatus(), events.get(i).fromStatus(),
                    "event chain broken at index " + i + " for job " + job.getId());
            assertTrue(events.get(i).sequence() > events.get(i - 1).sequence(),
                    "sequence not strictly increasing at index " + i + " for job " + job.getId());
        }
        assertEquals(job.getStatus(), events.get(events.size() - 1).toStatus(),
                "last event must match final job status for job " + job.getId());
    }
}
