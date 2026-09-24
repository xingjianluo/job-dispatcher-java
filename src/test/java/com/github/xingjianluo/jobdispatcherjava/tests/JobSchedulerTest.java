package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.Clock;
import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.domain.JobStatus;
import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import com.github.xingjianluo.jobdispatcherjava.domain.SubmitCommand;
import com.github.xingjianluo.jobdispatcherjava.handler.DefaultJobHandlerRegistry;
import com.github.xingjianluo.jobdispatcherjava.handler.JobContext;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandler;
import com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome;
import com.github.xingjianluo.jobdispatcherjava.service.CancelResult;
import com.github.xingjianluo.jobdispatcherjava.service.JobScheduler;
import com.github.xingjianluo.jobdispatcherjava.service.SubmitResult;
import com.github.xingjianluo.jobdispatcherjava.store.InMemoryJobStore;
import com.github.xingjianluo.jobdispatcherjava.store.JobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 调度核心语义单测。
 * 日志统一打印作业标识、状态流转与判定依据（见每个用例末尾的 dump 输出）。
 */
class JobSchedulerTest {

    private static final Logger log = LoggerFactory.getLogger(JobSchedulerTest.class);

    private JobStore store;
    private DefaultJobHandlerRegistry registry;
    private MutableClock clock;
    private DispatcherProperties props;
    private JobScheduler scheduler;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        registry = new DefaultJobHandlerRegistry(List.of());
        clock = new MutableClock(1_000_000L);
        props = new DispatcherProperties(2, 10, Duration.ofSeconds(30), 3,
                Duration.ofMillis(50), Duration.ofMillis(500),
                Duration.ofMillis(20), Duration.ofSeconds(10));
        RetryPolicy retry = new RetryPolicy(3, Duration.ofMillis(50), Duration.ofMillis(500));
        scheduler = JobScheduler.forTesting(store, registry, props, retry, clock);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private void register(JobHandler handler) {
        registry.register(handler);
    }

    private SubmitCommand cmd(String key, String handlerType, String payload, int priority) {
        return new SubmitCommand(key, handlerType, payload, Map.of(), priority);
    }

    private static JobHandler immediate(String type, AtomicInteger counter) {
        return new JobHandler() {
            @Override public String type() { return type; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                counter.incrementAndGet();
                return JobOutcome.success("ok");
            }
        };
    }

    private void dump(String caseName, JobRecord... jobs) {
        for (JobRecord job : jobs) {
            log.info("[{}] jobId={} key={} 最终状态={} failureType={} 尝试次数={} 事件流:",
                    caseName, job.getJobId(), job.getIdempotencyKey(),
                    job.getStatus(), job.getFailureType(), job.getAttempts());
            job.getEvents().forEach(e -> log.info("[{}]   {} -> {} | {}",
                    caseName, e.from() == null ? "-" : e.from(), e.to(), e.reason()));
        }
    }

    private void dump(String caseName, String jobId) {
        store.findByJobId(jobId).ifPresent(j -> dump(caseName, j));
    }

    private JobRecord awaitTerminal(String jobId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            JobRecord j = store.findByJobId(jobId).orElseThrow();
            if (j.getStatus().isTerminal()) {
                return j;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("作业未在时限内进入终态: " + jobId
                + store.findByJobId(jobId).map(JobRecord::getStatus).orElse(null));
    }

    @Test
    @DisplayName("同一幂等键重复提交只执行一次，并返回 DUPLICATE_SUBMISSION 原因")
    void duplicateSubmissionRunsOnce() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        register(immediate("echo", runs));

        SubmitResult first = scheduler.submit(cmd("idem-1", "echo", "a", 0));
        SubmitResult second = scheduler.submit(cmd("idem-1", "echo", "a", 0));
        SubmitResult third = scheduler.submit(cmd("idem-1", "echo", "a", 9));

        assertInstanceOf(SubmitResult.Accepted.class, first);
        assertInstanceOf(SubmitResult.Duplicate.class, second);
        assertInstanceOf(SubmitResult.Duplicate.class, third);
        assertEquals("DUPLICATE_SUBMISSION", second.reasonCode());

        JobRecord job = ((SubmitResult.Accepted) first).job();
        awaitTerminal(job.getJobId());
        // 终态后再次提交同样幂等键：仍然不得执行第二次
        SubmitResult afterTerminal = scheduler.submit(cmd("idem-1", "echo", "a", 0));
        assertInstanceOf(SubmitResult.Duplicate.class, afterTerminal);

        Thread.sleep(100);
        assertEquals(1, runs.get(), "同一幂等键只允许执行一次");
        dump("duplicateSubmissionRunsOnce", job);
    }

    @Test
    @DisplayName("容量不足时作业排队而非丢弃/超发；容量释放后按优先级降序、同优先级 FIFO 执行")
    void capacityQueuesAndPriorityOrders() throws Exception {
        // 容量=2：先放两个阻塞作业占满，后续作业必须排队
        BlockingHandler blocking = new BlockingHandler("block");
        CountDownLatch secondStarted = new CountDownLatch(1);
        register(blocking);

        SubmitResult a = scheduler.submit(cmd("p-low", "block", null, 1));
        // 等第一个真正开始后再提交，保证提交顺序确定
        assertTrue(blocking.started.await(5, TimeUnit.SECONDS));

        SubmitResult b = scheduler.submit(cmd("p-fifo1", "block", null, 5));
        SubmitResult c = scheduler.submit(cmd("p-high", "block", null, 10));
        SubmitResult d = scheduler.submit(cmd("p-fifo2", "block", null, 5));
        SubmitResult e = scheduler.submit(cmd("p-mid", "block", null, 7));

        assertInstanceOf(SubmitResult.Accepted.class, a);
        assertInstanceOf(SubmitResult.Accepted.class, b);
        assertInstanceOf(SubmitResult.Accepted.class, c);
        assertInstanceOf(SubmitResult.Accepted.class, d);
        assertInstanceOf(SubmitResult.Accepted.class, e);

        Thread.sleep(100);
        // 只有容量 2，实际开始执行次数必须为 2（不超发）
        assertEquals(2, blocking.startCount.get(), "容量不足时不得超发");
        assertEquals(5, store.size(), "所有作业都在系统中，未被丢弃");

        // 释放一个容量槽位；期望下一个拿到租约的是优先级最高的 p-high
        blocking.releaseOne();
        waitUntil(() -> blocking.startCount.get() == 3, 5_000);
        String third = currentLeasedExcept("p-low", "p-fifo1");
        assertEquals("p-high", third, "优先级 10 必须最先从排队中获得容量");

        // 再释放一个槽位 -> p-mid(7) 先于两个 priority=5
        blocking.releaseOne();
        waitUntil(() -> blocking.startCount.get() == 4, 5_000);
        String fourth = currentLeasedExcept("p-low", "p-fifo1", "p-high");
        assertEquals("p-mid", fourth, "优先级 7 先于优先级 5");

        // 同优先级 5 的两个作业按 FIFO：p-fifo1 在初次占容量时已在执行；
        // 排队的 p-fifo2 先提交，应先于其他同优先级作业（此处仅一个排队的 5）
        blocking.releaseOne();
        waitUntil(() -> blocking.startCount.get() == 5, 5_000);
        String fifth = currentLeasedExcept("p-low", "p-fifo1", "p-high", "p-mid");
        assertEquals("p-fifo2", fifth, "同优先级按提交顺序 FIFO");

        blocking.releaseAll();
        awaitTerminal(store.findByIdempotencyKey("p-low").orElseThrow().getJobId());
        awaitTerminal(store.findByIdempotencyKey("p-fifo1").orElseThrow().getJobId());
        awaitTerminal(store.findByIdempotencyKey("p-high").orElseThrow().getJobId());
        awaitTerminal(store.findByIdempotencyKey("p-fifo2").orElseThrow().getJobId());
        awaitTerminal(store.findByIdempotencyKey("p-mid").orElseThrow().getJobId());

        assertEquals(5, blocking.startCount.get());
        dump("capacityQueuesAndPriorityOrders",
                store.findByIdempotencyKey("p-fifo1").orElseThrow(),
                store.findByIdempotencyKey("p-high").orElseThrow(),
                store.findByIdempotencyKey("p-mid").orElseThrow(),
                store.findByIdempotencyKey("p-fifo2").orElseThrow());
    }

    @Test
    @DisplayName("等待队列超限拒绝入队，原因码 QUEUE_FULL，作业不产生执行")
    void queueFullIsRejectedWithReason() throws Exception {
        BlockingHandler blocking = new BlockingHandler("block");
        register(blocking);

        // 容量=2、队列上限=10：占满容量后再提交 10 个排队，第 11 个排队作业必须被拒绝
        scheduler.submit(cmd("cap-1", "block", null, 0));
        scheduler.submit(cmd("cap-2", "block", null, 0));
        assertTrue(blocking.started.await(5, TimeUnit.SECONDS));
        // 第二个开始信号也等到（两个容量都被占住）
        waitUntil(() -> blocking.startCount.get() == 2, 5_000);

        for (int i = 0; i < 10; i++) {
            assertInstanceOf(SubmitResult.Accepted.class,
                    scheduler.submit(cmd("wait-" + i, "block", null, 0)));
        }
        SubmitResult rejected = scheduler.submit(cmd("wait-overflow", "block", null, 0));
        assertInstanceOf(SubmitResult.RejectedQueueFull.class, rejected);
        assertEquals("QUEUE_FULL", rejected.reasonCode());
        assertTrue(rejected.reason().contains("等待队列已满"));
        assertEquals(12, store.size(), "被拒绝的作业不得进入存储，更不会执行");

        log.info("queueFullIsRejectedWithReason: 拒绝原因={}", rejected.reason());
    }

    @Test
    @DisplayName("未知处理器类型在入队前拒绝，原因码 UNKNOWN_HANDLER")
    void unknownHandlerRejectedBeforeEnqueue() {
        SubmitResult result = scheduler.submit(cmd("bad-handler", "not-exist", null, 0));
        assertInstanceOf(SubmitResult.RejectedUnknownHandler.class, result);
        assertEquals("UNKNOWN_HANDLER", result.reasonCode());
        assertEquals(0, store.size(), "作业未入队");
        log.info("unknownHandlerRejectedBeforeEnqueue: 拒绝原因={}", result.reason());
    }

    private void awaitLeased(String jobId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (store.findByJobId(jobId).orElseThrow().getStatus() == JobStatus.LEASED
                    || store.findByJobId(jobId).orElseThrow().getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("作业未进入 LEASED: " + jobId);
    }

    private String currentLeasedExcept(String... excludedKeys) throws InterruptedException {
        java.util.Set<String> excluded = java.util.Set.of(excludedKeys);
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            for (JobRecord j : store.findAll()) {
                if (j.getStatus() == JobStatus.LEASED && !excluded.contains(j.getIdempotencyKey())) {
                    return j.getIdempotencyKey();
                }
            }
            Thread.sleep(5);
        }
        throw new AssertionError("未观察到新的 LEASED 作业");
    }

    private static void waitUntil(CheckedCondition condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.matches()) {
                    return;
                }
            } catch (Exception ignored) {
                // 轮询直到满足
            }
            Thread.sleep(10);
        }
        throw new AssertionError("条件在时限内未满足");
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean matches() throws Exception;
    }
}
