package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.FailureType;
import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.domain.JobStatus;
import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import com.github.xingjianluo.jobdispatcherjava.domain.SubmitCommand;
import com.github.xingjianluo.jobdispatcherjava.handler.DefaultJobHandlerRegistry;
import com.github.xingjianluo.jobdispatcherjava.service.CancelResult;
import com.github.xingjianluo.jobdispatcherjava.service.JobScheduler;
import com.github.xingjianluo.jobdispatcherjava.service.SubmitResult;
import com.github.xingjianluo.jobdispatcherjava.store.InMemoryJobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 取消语义测试：取消/重复取消/对终态取消三者必须可判别。 */
class CancelTest {

    private static final Logger log = LoggerFactory.getLogger(CancelTest.class);

    private InMemoryJobStore store;
    private DefaultJobHandlerRegistry registry;
    private MutableClock clock;
    private JobScheduler scheduler;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        registry = new DefaultJobHandlerRegistry(List.of());
        clock = new MutableClock(4_000_000L);
        DispatcherProperties props = new DispatcherProperties(2, 100, Duration.ofMillis(300), 3,
                Duration.ofMillis(50), Duration.ofSeconds(10),
                Duration.ofMillis(10), Duration.ofMillis(50));
        RetryPolicy retry = new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(10));
        scheduler = JobScheduler.forTesting(store, registry, props, retry, clock);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private void dump(String caseName, JobRecord job) {
        log.info("[{}] jobId={} key={} 状态={} failureType={} 尝试次数={}",
                caseName, job.getJobId(), job.getIdempotencyKey(), job.getStatus(),
                job.getFailureType(), job.getAttempts());
        job.getEvents().forEach(e -> log.info("[{}]   {} -> {} | {}",
                caseName, e.from() == null ? "-" : e.from(), e.to(), e.reason()));
    }

    private JobRecord waitFor(String key, java.util.function.Predicate<JobRecord> pred, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            JobRecord j = store.findByIdempotencyKey(key).orElseThrow();
            if (pred.test(j)) {
                return j;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("条件未满足: " + key);
    }

    @Test
    @DisplayName("排队中取消 -> CANCELLED；再次取消 -> ALREADY_CANCELLED；成功作业取消 -> TERMINAL，三者可区分")
    void cancelStatesAreDistinguishable() throws Exception {
        BlockingHandler blocking = new BlockingHandler("block");
        registry.register(blocking);

        // 占满容量（2），第三个作业进入排队
        scheduler.submit(new SubmitCommand("run-1", "block", null, Map.of(), 0));
        scheduler.submit(new SubmitCommand("run-2", "block", null, Map.of(), 0));
        assertTrue(blocking.started.await(5, TimeUnit.SECONDS));
        while (blocking.startCount.get() < 2) {
            Thread.sleep(5);
        }
        SubmitResult queued = scheduler.submit(new SubmitCommand("q-1", "block", null, Map.of(), 0));
        assertInstanceOf(SubmitResult.Accepted.class, queued);
        String queuedId = queued.job().getJobId();
        Thread.sleep(50);
        assertEquals(JobStatus.QUEUED, store.findByJobId(queuedId).orElseThrow().getStatus());

        CancelResult first = scheduler.cancel(queuedId);
        assertInstanceOf(CancelResult.Cancelled.class, first);
        assertEquals(JobStatus.CANCELLED, first.job().getStatus());
        assertEquals(FailureType.CANCELLED, first.job().getFailureType());

        CancelResult second = scheduler.cancel(queuedId);
        assertInstanceOf(CancelResult.AlreadyCancelled.class, second, "重复取消必须可判别");
        assertEquals(JobStatus.CANCELLED, second.job().getStatus());

        // 对成功终态作业取消 -> Terminal，且终态为 SUCCEEDED
        blocking.releaseAll();
        waitFor("run-1", j -> j.getStatus() == JobStatus.SUCCEEDED, 5_000);
        String run1 = store.findByIdempotencyKey("run-1").orElseThrow().getJobId();
        CancelResult terminal = scheduler.cancel(run1);
        assertInstanceOf(CancelResult.Terminal.class, terminal, "终态作业取消必须可判别");
        assertEquals(JobStatus.SUCCEEDED, terminal.job().getStatus());

        // 被取消的排队作业永无执行机会
        Thread.sleep(200);
        dump("cancelStatesAreDistinguishable", store.findByJobId(queuedId).orElseThrow());
        assertEquals(0, store.findByJobId(queuedId).orElseThrow().getAttempts(),
                "排队阶段被取消的作业不得执行");
    }

    @Test
    @DisplayName("执行中取消：回收租约并进入 CANCELLED，迟到回执被拒，不出现 FAILED/EXHAUSTED")
    void cancelWhileLeasedReclaimsLease() throws Exception {
        BlockingHandler blocking = new BlockingHandler("block");
        registry.register(blocking);
        SubmitResult accepted = scheduler.submit(new SubmitCommand("run-x", "block", null, Map.of(), 0));
        String jobId = accepted.job().getJobId();
        assertTrue(blocking.started.await(5, TimeUnit.SECONDS));
        waitFor("run-x", j -> j.getStatus() == JobStatus.LEASED, 5_000);

        CancelResult cancelled = scheduler.cancel(jobId);
        assertInstanceOf(CancelResult.Cancelled.class, cancelled);
        JobRecord rec = store.findByJobId(jobId).orElseThrow();
        assertEquals(JobStatus.CANCELLED, rec.getStatus());
        assertNull(rec.getLeaseOwner(), "取消后租约应被回收");

        // 让被中断的执行方自然结束并交回执；调度器必须拒绝，状态保持 CANCELLED
        blocking.releaseAll();
        Thread.sleep(300);
        JobRecord stable = store.findByJobId(jobId).orElseThrow();
        assertEquals(JobStatus.CANCELLED, stable.getStatus());
        assertEquals(1, stable.getAttempts(), "取消不产生第二次执行尝试");
        dump("cancelWhileLeasedReclaimsLease", stable);
    }

    @Test
    @DisplayName("重试退避中取消：作业直接进入 CANCELLED，不再发生后续尝试")
    void cancelWhileWaitingRetryStopsRetries() throws Exception {
        // 本用例使用更长的初始退避，确保取消动作落在 WAITING_RETRY 窗口内
        DispatcherProperties localProps = new DispatcherProperties(2, 100, Duration.ofMillis(300), 3,
                Duration.ofSeconds(30), Duration.ofSeconds(60),
                Duration.ofMillis(10), Duration.ofMillis(50));
        RetryPolicy localRetry = new RetryPolicy(3, Duration.ofSeconds(30), Duration.ofSeconds(60));
        scheduler.shutdown();
        scheduler = JobScheduler.forTesting(store, registry, localProps, localRetry, clock);
        scheduler.start();

        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        registry.register(new com.github.xingjianluo.jobdispatcherjava.handler.JobHandler() {
            @Override public String type() { return "flaky"; }
            @Override public com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome handle(
                    String payload, com.github.xingjianluo.jobdispatcherjava.handler.JobContext context) {
                calls.incrementAndGet();
                return com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome.retryableFailure("超时");
            }
        });
        SubmitResult accepted = scheduler.submit(new SubmitCommand("retry-x", "flaky", null, Map.of(), 0));
        waitFor("retry-x", j -> j.getStatus() == JobStatus.WAITING_RETRY, 5_000);
        assertEquals(1, calls.get());

        CancelResult cancelled = scheduler.cancel(accepted.job().getJobId());
        assertInstanceOf(CancelResult.Cancelled.class, cancelled);

        // 真实等待远超一个扫描/调度周期：退避中被取消的作业不得再被调度
        Thread.sleep(300);
        JobRecord stable = store.findByJobId(accepted.job().getJobId()).orElseThrow();
        assertEquals(JobStatus.CANCELLED, stable.getStatus());
        assertEquals(1, calls.get(), "退避中取消后不得再尝试");
        dump("cancelWhileWaitingRetryStopsRetries", stable);
    }
}
