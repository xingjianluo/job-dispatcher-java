package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 失败分类，用于区分终态 FAILED 的具体原因。
 */
public enum FailureKind {
    /** 不可重试失败，直接终态 */
    NON_RETRYABLE,
    /** 可重试失败但重试次数耗尽 */
    RETRIES_EXHAUSTED,
    /** 执行方反复失联（租约过期被回收），尝试次数耗尽 */
    LOST_RETRIES_EXHAUSTED,
    /** 被主动取消 */
    CANCELLED
}
