package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 提交结果：携带判定分类与可解释原因。
 */
public record SubmitResult(SubmitOutcome outcome, Job job, String reason) {

    public static SubmitResult accepted(Job job) {
        return new SubmitResult(SubmitOutcome.ACCEPTED, job, "accepted and enqueued");
    }

    public static SubmitResult duplicate(Job job) {
        return new SubmitResult(SubmitOutcome.DUPLICATE, job,
                "idempotency key already exists: " + job.getIdempotencyKey());
    }

    public static SubmitResult rejected(String idempotencyKey, int capacity) {
        return new SubmitResult(SubmitOutcome.REJECTED_QUEUE_FULL, null,
                "queue capacity " + capacity + " reached, rejected idempotencyKey=" + idempotencyKey);
    }
}
