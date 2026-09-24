package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.FailureType;
import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.domain.JobStatus;
import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import com.github.xingjianluo.jobdispatcherjava.domain.SubmitCommand;
import com.github.xingjianluo.jobdispatcherjava.handler.DefaultJobHandlerRegistry;
import com.github.xingjianluo.jobdispatcherjava.handler.JobContext;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandler;
import com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发抢占与租约过期边界测试。
 * 关注点：任意时刻同一作业至多一个持有者、并发提交不重复执行、
 * 心跳续期、失联过期重分配、迟到回执/心跳被拒、过期不永久卡住。
 */
class LeaseAndPreemptionTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseAndPreemptionTest.class);

    private InMemoryJobStore store;
    private DefaultJobHandlerRegistry registry;
    private MutableClock clock;
    private JobScheduler scheduler;
    private DispatcherProperties props;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        registry = new DefaultJobHandlerRegistry(List.of());
        clock = new MutableClock(3_000_000L);
        // 租约 300ms：执行方每 100ms 心跳；后台扫描 50ms
        props = new DispatcherProperties(4, 100, Duration.ofMillis(300), 3,
                Duration.ofMillis(30), Duration.ofSeconds(10),
                Duration.ofMillis(10), Duration.ofMillis(50));
        RetryPolicy retry = new RetryPolicy(3, Duration.ofMillis(30), Duration.ofSeconds(10));
        scheduler = JobScheduler.forTesting(store, registry, props, retry, clock);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private SubmitCommand cmd(String key, String type) {
        return new SubmitCommand(key, type, null, Map.of(), 0);
    }

    /** 模拟执行方进程故障：不发送任何心跳（配合阻塞处理器制造失联）。 */
    private SubmitCommand silentCmd(String key, String type) {
        return new SubmitCommand(key, type, null,
                Map.of("dispatch.disable-heartbeat", "true"), 0);
    }

    private void dump(String caseName, JobRecord job) {
        log.info("[{}] jobId={} key={} 最终状态={} failureType={} 尝试次数={} owner={} epoch={}",
                caseName, job.getJobId(), job.getIdempotencyKey(), job.getStatus(),
                job.getFailureType(), job.getAttempts(), job.getLeaseOwner(), job.getLeaseEpoch());
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
        throw new AssertionError("条件未满足: " + key + " 状态="
                + store.findByIdempotencyKey(key).map(JobRecord::getStatus).orElse(null));
    }

    @Test
    @DisplayName("多线程并发提交同一幂等键：只产生一个作业，且只执行一次")
    void concurrentSubmitSameKeyRunsOnce() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        registry.register(new JobHandler() {
            @Override public String type() { return "once"; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                runs.incrementAndGet();
                return JobOutcome.success("ok");
            }
        });

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<SubmitResult> results = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(scheduler.submit(cmd("race-key", "once")));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        long accepted = results.stream().filter(r -> r instanceof SubmitResult.Accepted).count();
        long duplicates = results.stream().filter(r -> r instanceof SubmitResult.Duplicate).count();
        log.info("concurrentSubmitSameKeyRunsOnce: accepted={} duplicate={} storeSize={}",
                accepted, duplicates, store.size());
        assertEquals(1, accepted, "32 个并发提交中只能有 1 个被接收");
        assertEquals(threads - 1, duplicates);
        assertEquals(1, store.size());

        waitFor("race-key", j -> j.getStatus().isTerminal(), 5_000);
        Thread.sleep(100);
        assertEquals(1, runs.get(), "并发抢占下作业不得重复执行");
        dump("concurrentSubmitSameKeyRunsOnce", store.findByIdempotencyKey("race-key").orElseThrow());
    }

    @Test
    @DisplayName("租约有效期内执行方持续心跳：租约不被回收，作业只执行一次并成功")
    void heartbeatKeepsLeaseAlive() throws Exception {
        // 执行耗时超过租约时长，但持续心跳 -> 不应被判失联
        BlockingHandler handler = new BlockingHandler("long", 500);
        registry.register(handler);
        scheduler.submit(cmd("long-job", "long"));
        assertTrue(handler.started.await(5, TimeUnit.SECONDS));

        JobRecord leased = waitFor("long-job", j -> j.getStatus() == JobStatus.LEASED, 5_000);
        long epoch = leased.getLeaseEpoch();
        // 执行总耗时约 500ms+，租约 300ms：中途不应发生重分配
        Thread.sleep(450);
        JobRecord mid = store.findByIdempotencyKey("long-job").orElseThrow();
        assertEquals(JobStatus.LEASED, mid.getStatus(), "心跳保活期间不得回收租约");
        assertEquals(epoch, mid.getLeaseEpoch(), "代际不变意味着没有重分配");
        assertEquals(1, handler.startCount.get());

        handler.releaseOne();
        JobRecord done = waitFor("long-job", j -> j.getStatus() == JobStatus.SUCCEEDED, 5_000);
        assertEquals(1, handler.startCount.get());
        dump("heartbeatKeepsLeaseAlive", done);
    }

    @Test
    @DisplayName("执行方失联：租约过期后重新分配给新执行方，迟到回执被拒，作业最终成功")
    void expiredLeaseIsReassignedAndStaleCompletionRejected() throws Exception {
        // 第一轮持有者“卡死”：既不心跳也不退出（模拟失联/Full GC/网络分区）
        SilentBlockingHandler first = new SilentBlockingHandler("silent", false);
        registry.register(first);
        scheduler.submit(silentCmd("silent-job", "silent"));
        assertTrue(first.firstStarted.await(5, TimeUnit.SECONDS));

        JobRecord leased = waitFor("silent-job", j -> j.getStatus() == JobStatus.LEASED, 5_000);
        String firstOwner = leased.getLeaseOwner();
        long firstEpoch = leased.getLeaseEpoch();
        log.info("expiredLeaseCase: 首次发放 owner={} epoch={}，执行方不再心跳", firstOwner, firstEpoch);

        // 越过租约到期点（300ms），扫描器判定失联并回收
        JobRecord retryWaiting = waitFor("silent-job",
                j -> j.getStatus() == JobStatus.WAITING_RETRY, 5_000);
        assertEquals(FailureType.LEASE_EXPIRED, retryWaiting.getFailureType());
        log.info("expiredLeaseCase: 租约过期回收，failureType={} 退避至={}",
                retryWaiting.getFailureType(), retryWaiting.getReadyAt());

        // 替换成会成功的处理器（新执行方接替；原执行方仍卡死）
        SilentBlockingHandler second = new SilentBlockingHandler("silent", true);
        registry.replace(second);
        // 越过 30ms 退避
        clock.advanceMillis(20_000);

        JobRecord success = waitFor("silent-job", j -> j.getStatus() == JobStatus.SUCCEEDED, 5_000);
        assertEquals(2, success.getAttempts(), "过期后重新分配为第 2 次尝试");
        assertEquals(1, first.startCount.get(), "卡死的原执行方没有再次执行");
        assertEquals(1, second.startCount.get(), "新执行方恰好执行一次");

        // 原执行方“复活”并通过 WorkerTask 正常路径交回成功结果：必须按代际拒绝，
        // 不得改写已成功的作业（WorkerTask 的回执使用首次租约 epoch）
        first.resume();
        Thread.sleep(150);
        JobRecord afterStale = store.findByIdempotencyKey("silent-job").orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, afterStale.getStatus(), "迟到回执不得污染终态");
        dump("expiredLeaseIsReassignedAndStaleCompletionRejected", afterStale);
    }

    @Test
    @DisplayName("执行方反复失联：过期按失败计数，达到上限进入 EXHAUSTED/LEASE_EXPIRED，不永久卡住")
    void repeatedlyExpiredLeaseBoundedToExhausted() throws Exception {
        SilentBlockingHandler alwaysSilent = new SilentBlockingHandler("ghost", false);
        registry.register(alwaysSilent);
        scheduler.submit(silentCmd("ghost-job", "ghost"));
        assertTrue(alwaysSilent.firstStarted.await(5, TimeUnit.SECONDS));

        // 每次租约过期 -> 退避 -> 重分配；3 次尝试后必须终态
        JobRecord exhausted = waitFor("ghost-job",
                j -> j.getStatus() == JobStatus.EXHAUSTED, 10_000);
        assertEquals(3, exhausted.getAttempts(), "首次+2次过期重试，共 3 次尝试");
        assertEquals(FailureType.LEASE_EXPIRED, exhausted.getFailureType(),
                "租约过期耗尽须与普通重试耗尽可区分");
        assertEquals(3, alwaysSilent.startCount.get(), "每次重分配都是一次真实执行尝试");

        // 终态后再等两个扫描周期，状态不得回退、尝试次数不再增加
        Thread.sleep(400);
        JobRecord stable = store.findByIdempotencyKey("ghost-job").orElseThrow();
        assertEquals(JobStatus.EXHAUSTED, stable.getStatus());
        assertEquals(3, alwaysSilent.startCount.get());
        dump("repeatedlyExpiredLeaseBoundedToExhausted", stable);
    }

    static class SilentBlockingHandler implements JobHandler {
        private final String type;
        private final boolean succeedOnRun;
        /** 多次执行尝试各自独立的放行闩（失联场景下同类型 handler 会被多个 worker 进入）。 */
        private final java.util.concurrent.ConcurrentHashMap<Integer, CountDownLatch> releases =
                new java.util.concurrent.ConcurrentHashMap<>();
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final AtomicInteger startCount = new AtomicInteger();
        private volatile JobContext lastContext;

        SilentBlockingHandler(String type, boolean succeedOnRun) {
            this.type = type;
            this.succeedOnRun = succeedOnRun;
        }

        @Override public String type() { return type; }

        @Override
        public JobOutcome handle(String payload, JobContext context) {
            lastContext = context;
            int n = startCount.incrementAndGet();
            CountDownLatch mine = new CountDownLatch(1);
            releases.put(n, mine);
            if (n == 1) {
                firstStarted.countDown();
            }
            if (succeedOnRun) {
                return JobOutcome.success("接替执行方成功");
            }
            try {
                mine.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return JobOutcome.success("迟来的成功");
        }

        /** 放行第一次执行（模拟失联执行方在租约易主后才苏醒）。 */
        void resume() {
            CountDownLatch latch = releases.get(1);
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
