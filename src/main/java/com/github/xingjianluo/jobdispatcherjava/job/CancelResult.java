package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 取消结果：携带三态判定分类、作业快照与可解释原因。
 */
public record CancelResult(CancelOutcome outcome, Job job, String reason) {

    /** 兼容旧语义：仅当本次取消生效时为 true。 */
    public boolean effective() {
        return outcome == CancelOutcome.CANCELLED;
    }

    public static CancelResult cancelled(Job job) {
        return new CancelResult(CancelOutcome.CANCELLED, job, "cancel effective, job is now terminal CANCELLED");
    }

    public static CancelResult alreadyCancelled(Job job) {
        return new CancelResult(CancelOutcome.ALREADY_CANCELLED, job,
                "job was already cancelled before, no state change");
    }

    public static CancelResult alreadyTerminal(Job job) {
        return new CancelResult(CancelOutcome.ALREADY_TERMINAL, job,
                "job already reached terminal status " + job.getStatus()
                        + " (failureKind=" + job.getFailureKind() + "), cancel not effective");
    }
}
