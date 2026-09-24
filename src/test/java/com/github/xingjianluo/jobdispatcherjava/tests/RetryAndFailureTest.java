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
import com.github.xingjianluo.jobdispatcherjava.handler.NonRetryableJobException;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 失败分类与有界退避重试单测。 */
class RetryAndFailureTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndFailureTest.class);

    private InMemoryJobStore store;
    private DefaultJobHandlerRegistry registry;
    private MutableClock clock;
    private JobScheduler scheduler;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        registry = new DefaultJobHandlerRegistry(List.of());
        clock = new MutableClock(2_000_000L);
        // 最大 3 次尝试（首次 + 2 次重试），退避 50ms 起步（受控时钟不推动则重试不会就绪）
        DispatcherProperties props = new DispatcherProperties(2, 100, Duration.ofSeconds(30), 3,
                Duration.ofMillis(50), Duration.ofSeconds(10),
                Duration.ofMillis(10), Duration.ofSeconds(10));
        RetryPolicy retry = new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(10));
        scheduler = JobScheduler.forTesting(store, registry, props, retry, clock);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private SubmitCommand cmd(String key, String type, String payload) {
        return new SubmitCommand(key, type, payload, Map.of(), 0);
    }

    private JobRecord awaitStatus(String key, JobStatus status) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            JobRecord j = store.findByIdempotencyKey(key).orElseThrow();
            if (j.getStatus() == status) {
                return j;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("作业未到达状态 " + status + ": "
                + store.findByIdempotencyKey(key).map(JobRecord::getStatus).orElse(null));
    }

    private void dump(String caseName, JobRecord job) {
        log.info("[{}] jobId={} key={} 最终状态={} failureType={} 尝试次数={} result={} failureMessage={}",
                caseName, job.getJobId(), job.getIdempotencyKey(), job.getStatus(),
                job.getFailureType(), job.getAttempts(), job.getResultPayload(), job.getFailureMessage());
        job.getEvents().forEach(e -> log.info("[{}]   {} -> {} | {}",
                caseName, e.from() == null ? "-" : e.from(), e.to(), e.reason()));
    }

    @Test
    @DisplayName("不可重试失败直接进入 FAILED/NON_RETRYABLE，不重试")
    void nonRetryableFailsImmediately() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        registry.register(new JobHandler() {
            @Override public String type() { return "fatal"; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                calls.incrementAndGet();
                throw new NonRetryableJobException("业务永久拒绝：余额不足");
            }
        });
        SubmitResult result = scheduler.submit(cmd("k-fatal", "fatal", null));
        assertInstanceOf(SubmitResult.Accepted.class, result);

        JobRecord job = awaitStatus("k-fatal", JobStatus.FAILED);
        assertEquals(1, calls.get(), "不可重试失败不得重试");
        assertEquals(FailureType.NON_RETRYABLE, job.getFailureType());
        assertTrue(job.getFailureMessage().contains("余额不足"));
        dump("nonRetryableFailsImmediately", job);
    }

    @Test
    @DisplayName("可重试失败按有界次数退避重试，耗尽后进入 EXHAUSTED/RETRY_EXHAUSTED")
    void retryableFailureIsBoundedAndExhausted() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        registry.register(new JobHandler() {
            @Override public String type() { return "flaky"; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                int n = calls.incrementAndGet();
                log.info("flaky handler 第{}次被调用 attempt={} jobId={}", n, context.attempt(), context.jobId());
                return JobOutcome.retryableFailure("下游超时");
            }
        });
        scheduler.submit(cmd("k-flaky", "flaky", null));

        // 第 1 次失败 -> WAITING_RETRY；首次退避约 50ms，此前不应出现第 2 次尝试
        JobRecord waiting = awaitStatus("k-flaky", JobStatus.WAITING_RETRY);
        long readyAt = waiting.getReadyAt();
        assertTrue(readyAt >= clock.nowMillis(), "退避未到期时不得重试");
        // 等到真实时间接近（但不越过）退避点，确认仍然只有 1 次尝试
        long waitMillis = Math.max(0, readyAt - clock.nowMillis() - 5);
        Thread.sleep(waitMillis);
        assertEquals(1, calls.get(), "退避窗口内不允许第二次尝试");

        // 推进时钟越过退避点 -> 第 2、3 次尝试依次发生并仍失败 -> EXHAUSTED
        clock.advanceMillis(20_000);
        JobRecord exhausted = awaitStatus("k-flaky", JobStatus.EXHAUSTED);
        assertEquals(3, calls.get(), "总执行次数必须恰好为最大尝试次数 3");
        assertEquals(3, exhausted.getAttempts());
        assertEquals(FailureType.RETRY_EXHAUSTED, exhausted.getFailureType());
        assertTrue(exhausted.getFailureMessage().contains("重试次数耗尽"));

        // EXHAUSTED 是终态：推进再久也不再执行
        clock.advanceMillis(100_000);
        Thread.sleep(100);
        assertEquals(3, calls.get());
        dump("retryableFailureIsBoundedAndExhausted", exhausted);
    }

    @Test
    @DisplayName("退避期间恢复成功：重试尝试成功后进入 SUCCEEDED")
    void retryThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        registry.register(new JobHandler() {
            @Override public String type() { return "recover"; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                int n = calls.incrementAndGet();
                return n < 3 ? JobOutcome.retryableFailure("临时抖动") : JobOutcome.success("恢复成功");
            }
        });
        scheduler.submit(cmd("k-recover", "recover", null));
        awaitStatus("k-recover", JobStatus.WAITING_RETRY);
        clock.advanceMillis(20_000);
        JobRecord done = awaitStatus("k-recover", JobStatus.SUCCEEDED);
        assertEquals(3, calls.get());
        assertEquals("恢复成功", done.getResultPayload());
        dump("retryThenSucceeds", done);
    }

    @Test
    @DisplayName("处理器抛出未预期异常（非分类异常）按可重试处理，不静默吞掉、不直接透出")
    void unexpectedExceptionIsClassifiedRetryable() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        registry.register(new JobHandler() {
            @Override public String type() { return "boom"; }
            @Override public JobOutcome handle(String payload, JobContext context) {
                calls.incrementAndGet();
                throw new IllegalStateException("simulated low-level NPE-like failure\n\tat internal.Foo.bar(Foo.java:1)");
            }
        });
        scheduler.submit(cmd("k-boom", "boom", null));
        awaitStatus("k-boom", JobStatus.WAITING_RETRY);
        JobRecord waiting = store.findByIdempotencyKey("k-boom").orElseThrow();
        assertEquals(FailureType.RETRYABLE, waiting.getFailureType());
        // 对外保存的是规整说明，不包含底层异常类名/堆栈
        assertFalse(waiting.getFailureMessage().contains("IllegalStateException"));
        assertFalse(waiting.getFailureMessage().contains("internal.Foo"));
        clock.advanceMillis(20_000);
        JobRecord exhausted = awaitStatus("k-boom", JobStatus.EXHAUSTED);
        assertEquals(3, calls.get());
        dump("unexpectedExceptionIsClassifiedRetryable", exhausted);
    }
}
