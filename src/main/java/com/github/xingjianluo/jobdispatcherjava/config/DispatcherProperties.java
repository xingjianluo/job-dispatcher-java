package com.github.xingjianluo.jobdispatcherjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 调度器可调参数（前缀 {@code job-dispatcher}）。
 *
 * @param maxConcurrent  最大并发执行容量
 * @param maxQueueSize   等待队列容量；超过则拒绝（返回 429）
 * @param leaseDuration  执行租约时长；执行方按其 1/3 周期心跳续期
 * @param maxAttempts    最大执行次数（首次 + 重试）
 * @param initialBackoff 首次重试基础退避（按指数翻倍并加入抖动）
 * @param maxBackoff     退避上限
 * @param pollInterval   调度扫描间隔
 * @param sweepInterval  过期租约扫描间隔
 */
@ConfigurationProperties(prefix = "job-dispatcher")
public record DispatcherProperties(int maxConcurrent,
                                   int maxQueueSize,
                                   Duration leaseDuration,
                                   int maxAttempts,
                                   Duration initialBackoff,
                                   Duration maxBackoff,
                                   Duration pollInterval,
                                   Duration sweepInterval) {

    public DispatcherProperties {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("maxConcurrent must be positive: " + maxConcurrent);
        }
        if (maxQueueSize <= 0) {
            throw new IllegalArgumentException("maxQueueSize must be positive: " + maxQueueSize);
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        if (initialBackoff == null || initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff must not be negative");
        }
        if (maxBackoff == null || maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= initialBackoff");
        }
        if (pollInterval == null || pollInterval.isZero() || pollInterval.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        if (sweepInterval == null || sweepInterval.isZero() || sweepInterval.isNegative()) {
            throw new IllegalArgumentException("sweepInterval must be positive");
        }
    }

    /** 与 application.properties 中默认值保持一致的构造入口。 */
    public static DispatcherProperties defaults() {
        return new DispatcherProperties(4, 1000, Duration.ofSeconds(30), 3,
                Duration.ofSeconds(2), Duration.ofSeconds(60),
                Duration.ofMillis(100), Duration.ofMillis(200));
    }
}
