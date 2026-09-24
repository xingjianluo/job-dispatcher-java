package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.handler.JobContext;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandler;
import com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用阻塞处理器：进入执行后在 started 上放行，在 release 上等待，
 * 用于精确制造“租约发放后执行方仍占着”的场景；统计实际开始执行次数以证明不重复执行。
 */
class BlockingHandler implements JobHandler {

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch releaseAll = new CountDownLatch(1);
    /** 每次进入执行生成一个独立放行闩，支持逐个释放容量槽位。 */
    final java.util.concurrent.LinkedBlockingQueue<CountDownLatch> perRunRelease =
            new java.util.concurrent.LinkedBlockingQueue<>();
    final AtomicInteger startCount = new AtomicInteger();
    final AtomicInteger endCount = new AtomicInteger();
    private final String type;
    private final long sleepMillisOnStart;

    BlockingHandler(String type) {
        this(type, 0);
    }

    BlockingHandler(String type, long sleepMillisOnStart) {
        this.type = type;
        this.sleepMillisOnStart = sleepMillisOnStart;
    }

    /** 放行最早一个尚未结束的执行。 */
    void releaseOne() {
        CountDownLatch latch = perRunRelease.poll();
        if (latch != null) {
            latch.countDown();
        }
    }

    /** 放行全部执行。 */
    void releaseAll() {
        releaseAll.countDown();
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public JobOutcome handle(String payload, JobContext context) {
        startCount.incrementAndGet();
        started.countDown();
        CountDownLatch mine = new CountDownLatch(1);
        perRunRelease.add(mine);
        try {
            if (sleepMillisOnStart > 0) {
                Thread.sleep(sleepMillisOnStart);
            }
            // 任一通道触发都可退出
            while (mine.getCount() > 0 && releaseAll.getCount() > 0) {
                if (mine.await(50, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new com.github.xingjianluo.jobdispatcherjava.handler.RetryableJobException(
                    "被中断（模拟执行方响应租约失效/取消）", e);
        } finally {
            endCount.incrementAndGet();
        }
        return JobOutcome.success("done");
    }
}
