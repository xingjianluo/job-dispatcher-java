package com.github.xingjianluo.jobdispatcherjava.service;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.handler.JobContext;
import com.github.xingjianluo.jobdispatcherjava.handler.JobHandler;
import com.github.xingjianluo.jobdispatcherjava.handler.JobOutcome;
import com.github.xingjianluo.jobdispatcherjava.handler.NonRetryableJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一次执行尝试的运行壳：执行方身份、心跳续期、回执回调全部绑定到特定租约代际，
 * 使过期持有者的迟到心跳/回执可被识别并拒绝，杜绝重复执行。
 */
class WorkerTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);

    private final JobRecord job;
    private final JobHandler handler;
    private final String workerId;
    private final int attempt;
    private final long leaseEpoch;
    private final long leaseDurationMillis;
    private final HeartbeatSink heartbeatSink;
    private final CompletionSink completionSink;
    private final CancelCheck cancelCheck;
    private final ScheduledExecutorService heartbeatExecutor;

    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile Thread workerThread;
    private final boolean heartbeatEnabled;

    WorkerTask(JobRecord job,
               JobHandler handler,
               String workerId,
               int attempt,
               long leaseEpoch,
               long leaseDurationMillis,
               boolean heartbeatEnabled,
               HeartbeatSink heartbeatSink,
               CompletionSink completionSink,
               CancelCheck cancelCheck,
               ScheduledExecutorService heartbeatExecutor) {
        this.job = job;
        this.handler = handler;
        this.workerId = workerId;
        this.attempt = attempt;
        this.leaseEpoch = leaseEpoch;
        this.leaseDurationMillis = leaseDurationMillis;
        this.heartbeatEnabled = heartbeatEnabled;
        this.heartbeatSink = heartbeatSink;
        this.completionSink = completionSink;
        this.cancelCheck = cancelCheck;
        this.heartbeatExecutor = heartbeatExecutor;
    }

    @Override
    public void run() {
        workerThread = Thread.currentThread();
        AttemptResult result;
        // 租约 1/3 周期心跳；心跳被拒（代际不符/已取消）则中断执行线程。
        // heartbeatEnabled=false 用于测试模拟执行方进程死掉（心跳彻底停止）。
        long heartbeatInterval = Math.max(1L, leaseDurationMillis / 3);
        if (heartbeatEnabled) {
            heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(this::heartbeat,
                    heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
        }
        try {
            JobContext context = new Context();
            JobOutcome outcome = handler.handle(job.getPayload(), context);
            result = switch (outcome.status()) {
                case SUCCESS -> AttemptResult.success(outcome.message());
                case RETRYABLE_FAILURE ->
                        AttemptResult.failure(true, sanitize(outcome.message()), null);
                case NON_RETRYABLE_FAILURE ->
                        AttemptResult.failure(false, sanitize(outcome.message()), null);
            };
        } catch (NonRetryableJobException ex) {
            log.warn("jobId={} worker={} epoch={} attempt={} 不可重试异常: {}",
                    job.getJobId(), workerId, leaseEpoch, attempt, ex.toString());
            result = AttemptResult.failure(false, sanitize(ex.getMessage()), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("jobId={} worker={} epoch={} attempt={} 执行被中断（租约失效或取消）",
                    job.getJobId(), workerId, leaseEpoch, attempt);
            result = AttemptResult.failure(true, "执行被中断：租约失效或作业取消", ex);
        } catch (Throwable ex) {
            // 兜底：未预期异常一律按可重试处理并完整记录，绝不静默吞掉
            log.warn("jobId={} worker={} epoch={} attempt={} 抛出未预期异常，按可重试失败处理",
                    job.getJobId(), workerId, leaseEpoch, attempt, ex);
            result = AttemptResult.failure(true, "执行抛出未预期异常，已按可重试失败处理", ex);
        } finally {
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
            }
        }
        reportOnce(result);
    }

    private void heartbeat() {
        try {
            boolean valid = heartbeatSink.heartbeat(job.getJobId(), workerId, leaseEpoch);
            if (!valid) {
                log.warn("jobId={} worker={} epoch={} 心跳被拒：租约已失效（取消或过期重分配），中断执行",
                        job.getJobId(), workerId, leaseEpoch);
                if (heartbeatFuture != null) {
                    heartbeatFuture.cancel(false);
                }
                // 无法持有 WorkerTask 所在线程引用；执行方通过 cancelled() 轮询，
                // 同时这里靠“回执将被代际校验拒绝”保证不会污染新持有者的作业
                Thread t = workerThread;
                if (t != null) {
                    t.interrupt();
                }
            }
        } catch (Throwable t) {
            log.warn("jobId={} worker={} epoch={} 心跳异常: {}",
                    job.getJobId(), workerId, leaseEpoch, t.toString());
        }
    }

    private void reportOnce(AttemptResult result) {
        // 无论发生什么，只回执一次；最终是否被接受由调度器按 (epoch, owner) 校验
        if (finished.compareAndSet(false, true)) {
            completionSink.complete(job.getJobId(), workerId, leaseEpoch, result);
        }
    }

    private static String sanitize(String message) {
        if (message == null) {
            return "执行失败（无详细说明）";
        }
        // 不做堆栈/类名透出；仅保留处理器给出的业务说明
        return message.length() > 500 ? message.substring(0, 500) + "...(truncated)" : message;
    }

    private final class Context implements JobContext {
        @Override public String jobId() { return job.getJobId(); }
        @Override public int attempt() { return attempt; }
        @Override public long leaseEpoch() { return leaseEpoch; }

        @Override
        public boolean cancelled() {
            return cancelCheck.isCancelled(job.getJobId());
        }
    }

    /** 心跳续期回调。返回 false 表示租约已不属于本执行方，应停止工作。 */
    @FunctionalInterface
    interface HeartbeatSink {
        boolean heartbeat(String jobId, String workerId, long leaseEpoch);
    }

    /** 执行结论回执。 */
    record AttemptResult(boolean success, boolean retryable, String message, Throwable rawError) {
        static AttemptResult success(String message) {
            return new AttemptResult(true, false, message, null);
        }

        static AttemptResult failure(boolean retryable, String message, Throwable rawError) {
            return new AttemptResult(false, retryable, message, rawError);
        }
    }

    @FunctionalInterface
    interface CompletionSink {
        void complete(String jobId, String workerId, long leaseEpoch, AttemptResult result);
    }

    /** 供 {@link JobContext} 实现查询取消状态。 */
    @FunctionalInterface
    interface CancelCheck {
        boolean isCancelled(String jobId);
    }
}
