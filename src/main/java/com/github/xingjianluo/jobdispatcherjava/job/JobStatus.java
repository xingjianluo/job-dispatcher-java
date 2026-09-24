package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 作业生命周期状态。
 */
public enum JobStatus {
    /** 已入队，等待被租约获取 */
    QUEUED,
    /** 已被某执行方持有租约，正在执行 */
    LEASED,
    /** 执行成功（终态） */
    SUCCEEDED,
    /** 执行失败（终态，细分原因见 FailureKind） */
    FAILED,
    /** 已取消（终态） */
    CANCELLED
}
