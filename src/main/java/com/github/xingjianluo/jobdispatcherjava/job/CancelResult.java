package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 取消结果：携带三态判定分类、作业快照与可解释原因。
 */
public record CancelResult(CancelOutcome outcome, Job job, String reason) {

    /** 本次取消是否生效（兼容旧的 boolean 语义）。 */
    public boolean effective() {
        return outcome == CancelOutcome.CANCELLED;
    }
}
