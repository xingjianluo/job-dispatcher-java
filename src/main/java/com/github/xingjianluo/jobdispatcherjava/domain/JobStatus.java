package com.github.xingjianluo.jobdispatcherjava.domain;

/**
 * 作业生命周期状态。
 *
 * <pre>
 * QUEUED -> LEASED -> SUCCEEDED
 *                  -> FAILED（不可重试）
 *                  -> WAITING_RETRY -> LEASED ...
 *                  -> EXHAUSTED（重试次数耗尽，含租约过期导致的重试耗尽）
 * QUEUED / WAITING_RETRY / LEASED -> CANCELLED
 * </pre>
 */
public enum JobStatus {
    /** 已入队，等待容量。 */
    QUEUED,
    /** 已被某个执行方持有租约，正在执行。 */
    LEASED,
    /** 成功终态。 */
    SUCCEEDED,
    /** 不可重试失败终态。 */
    FAILED,
    /** 可重试失败，处于退避等待中。 */
    WAITING_RETRY,
    /** 可重试失败但重试次数耗尽的终态。 */
    EXHAUSTED,
    /** 被取消的终态（含排队中、退避中、租约执行中的取消）。 */
    CANCELLED;

    public boolean isTerminal() {
        return switch (this) {
            case SUCCEEDED, FAILED, EXHAUSTED, CANCELLED -> true;
            case QUEUED, LEASED, WAITING_RETRY -> false;
        };
    }
}
