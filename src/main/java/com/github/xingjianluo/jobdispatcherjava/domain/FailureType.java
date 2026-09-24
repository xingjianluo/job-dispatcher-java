package com.github.xingjianluo.jobdispatcherjava.domain;

/** 失败分类，用于区分终态与排障解释。 */
public enum FailureType {
    /** 无失败。 */
    NONE,
    /** 不可重试失败（业务显式拒绝、参数不合法等）。 */
    NON_RETRYABLE,
    /** 可重试失败（瞬时错误）。 */
    RETRYABLE,
    /** 可重试失败但重试次数已耗尽。 */
    RETRY_EXHAUSTED,
    /** 执行方租约过期（失联/超时），按可重试失败计数。 */
    LEASE_EXPIRED,
    /** 作业被取消。 */
    CANCELLED
}
