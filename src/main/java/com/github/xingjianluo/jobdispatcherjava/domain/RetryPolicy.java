package com.github.xingjianluo.jobdispatcherjava.domain;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 有界退避重试策略。
 *
 * @param maxAttempts   最大执行次数（首次执行 + 重试次数），&gt;= 1
 * @param initialBackoff 首次重试前的基础退避
 * @param maxBackoff    退避上限
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {

    /**
     * 第 {@code attempt} 次执行失败后（attempt 从 1 开始）需要等待多久再重试。
     * 指数退避 {@code initial * 2^(attempt-1)}，叠加 [0, 50%) 抖动，并封顶在 maxBackoff。
     */
    public Duration backoffAfterAttempt(int attempt) {
        long baseMillis = Math.max(1L, initialBackoff.toMillis());
        long capMillis = Math.max(baseMillis, maxBackoff.toMillis());
        int shift = Math.max(0, attempt - 1);
        long exponential = shift >= 63 ? Long.MAX_VALUE : baseMillis << shift;
        long bounded = Math.min(capMillis, exponential);
        // 抖动：在 [bounded/2, bounded] 区间取值，避免重试同拍
        long jittered = bounded / 2 + ThreadLocalRandom.current().nextLong(Math.max(1L, bounded / 2 + 1));
        return Duration.ofMillis(Math.min(capMillis, jittered));
    }
}
