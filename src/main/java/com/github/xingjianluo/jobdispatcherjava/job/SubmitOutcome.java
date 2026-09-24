package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 提交结果分类，使排队/拒绝/去重的原因可解释。
 */
public enum SubmitOutcome {
    /** 新作业，已接受并入队 */
    ACCEPTED,
    /** 幂等标识已存在，返回既有作业，不产生重复执行 */
    DUPLICATE,
    /** 队列容量不足，被拒绝 */
    REJECTED_QUEUE_FULL
}
