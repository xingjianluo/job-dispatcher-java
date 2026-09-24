package com.github.xingjianluo.jobdispatcherjava.handler;

/**
 * 执行方对一次执行尝试的结论。
 *
 * @param status  结论
 * @param message 结果或失败说明（对外透出前会被规整，不直接暴露底层异常）
 */
public record JobOutcome(Status status, String message) {

    public enum Status {
        SUCCESS,
        RETRYABLE_FAILURE,
        NON_RETRYABLE_FAILURE
    }

    public static JobOutcome success(String message) {
        return new JobOutcome(Status.SUCCESS, message);
    }

    public static JobOutcome retryableFailure(String message) {
        return new JobOutcome(Status.RETRYABLE_FAILURE, message);
    }

    public static JobOutcome nonRetryableFailure(String message) {
        return new JobOutcome(Status.NON_RETRYABLE_FAILURE, message);
    }
}
