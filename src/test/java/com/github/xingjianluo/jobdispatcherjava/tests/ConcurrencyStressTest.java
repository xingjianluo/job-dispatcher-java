package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 高并发压力：多作业 × 多线程重复提交，校验不丢失、不重复执行、终态正确。 */
class ConcurrencyStressTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyStressTest.class);

    private InMemoryJobStore store;
    private DefaultJobHandlerRegistry registry;
    private JobScheduler scheduler;

    private final Map<String, AtomicInteger> perKeyRuns = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        registry = new DefaultJobHandlerRegistry(java.util.List.of());
        MutableClock clock = new MutableClock(5_000_000L);
        DispatcherProperties props = new DispatcherProperties(8, 5000, Duration.ofSeconds(30), 1,
                Duration.ofMillis(10), Duration.ofSeconds(2),
                Duration.ofMillis(5), Duration.ofMillis(50));
        RetryPolicy retry = new RetryPolicy(1, Duration.ofMillis(10), Duration.ofSeconds(2));
        scheduler = JobScheduler.forTesting(store, registry, props, retry, clock);
        registry.register(new CountingHandler());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    class CountingHandler implements JobHandler {
        @Override public String type() { return "counting"; }
        @Override
        public JobOutcome handle(String payload, JobContext context) {
            perKeyRuns.computeIfAbsent(payload, k -> new AtomicInteger()).incrementAndGet();
            return JobOutcome.success("ok");
        }
    }

    @Test
    @DisplayName("50 个作业 × 每键 8 个并发线程提交：每键恰好接收 1 次、执行 1 次、最终成功")
    void stressNoDuplicateNoLoss() throws Exception {
        int jobs = 50;
        int submittersPerKey = 8;
        int threads = jobs * submittersPerKey;
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger acceptedCount = new AtomicInteger();
        AtomicInteger duplicateCount = new AtomicInteger();

        for (int j = 0; j < jobs; j++) {
            String key = "stress-key-" + j;
            for (int t = 0; t < submittersPerKey; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        SubmitResult r = scheduler.submit(
                                new SubmitCommand(key, "counting", key, Map.of(), 0));
                        if (r instanceof SubmitResult.Accepted) {
                            acceptedCount.incrementAndGet();
                        } else if (r instanceof SubmitResult.Duplicate) {
                            duplicateCount.incrementAndGet();
                        } else {
                            fail("不应出现排队拒绝以外的结果: " + r);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(jobs, acceptedCount.get(), "接收数必须恰好等于不同作业数");
        assertEquals(threads - jobs, duplicateCount.get());
        assertEquals(jobs, store.size());

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            long succeeded = store.findAll().stream()
                    .filter(j -> j.getStatus() == JobStatus.SUCCEEDED).count();
            if (succeeded == jobs) {
                break;
            }
            Thread.sleep(20);
        }

        for (int j = 0; j < jobs; j++) {
            String key = "stress-key-" + j;
            JobRecord rec = store.findByIdempotencyKey(key).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, rec.getStatus(), key + " 必须成功且不丢失");
            assertEquals(1, rec.getAttempts(), key + " 只允许执行一次");
            assertEquals(1, perKeyRuns.getOrDefault(key, new AtomicInteger()).get(),
                    key + " 处理器只允许被调用一次");
        }
        log.info("stressNoDuplicateNoLoss 完成: 作业数={} 接收={} 去重={} 总执行次数={}",
                jobs, acceptedCount.get(), duplicateCount.get(),
                perKeyRuns.values().stream().mapToInt(AtomicInteger::get).sum());
    }
}
