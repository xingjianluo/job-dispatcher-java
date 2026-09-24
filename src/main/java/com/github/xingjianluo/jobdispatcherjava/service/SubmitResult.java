package com.github.xingjianluo.jobdispatcherjava.service;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;

/**
 * 提交入口的判别式结果。每种取值都带有可解释原因。
 */
public sealed interface SubmitResult {

    /** 机器可读原因码。 */
    String reasonCode();

    /** 人类可读说明（调度顺序/排队/拒绝依据）。 */
    String reason();

    JobRecord job();

    /** 新提交并已进入队列。 */
    record Accepted(JobRecord job, int queuePosition, int queueSize, int activeLeases, int capacity)
            implements SubmitResult {
        @Override public String reasonCode() { return "ACCEPTED"; }
        @Override public String reason() {
            return "作业已入队；当前排队位置=%d，队列长度=%d，在途租约=%d/%d，按优先级(高->低)与提交顺序调度"
                    .formatted(queuePosition, queueSize, activeLeases, capacity);
        }
    }

    /** 同一幂等标识已存在（无论是否终态），直接返回既有作业，绝不二次执行。 */
    record Duplicate(JobRecord job) implements SubmitResult {
        @Override public String reasonCode() { return "DUPLICATE_SUBMISSION"; }
        @Override public String reason() {
            return "幂等标识已存在（当前状态=%s），复用既有作业，不产生新的执行"
                    .formatted(job().getStatus());
        }
    }

    /** 队列容量已满（在途容量占满不算拒绝，作业仍会排队；仅等待队列超长才拒绝）。 */
    record RejectedQueueFull(JobRecord job, int queueSize) implements SubmitResult {
        @Override public String reasonCode() { return "QUEUE_FULL"; }
        @Override public String reason() {
            return "等待队列已满（长度=%d），本次提交被拒绝，作业未入队、不会执行".formatted(queueSize);
        }
    }

    /** 引用的处理器类型不存在（不可重试前提不满足，拒绝入队）。 */
    record RejectedUnknownHandler(JobRecord job, String handlerType) implements SubmitResult {
        @Override public String reasonCode() { return "UNKNOWN_HANDLER"; }
        @Override public String reason() {
            return "处理器类型不存在: %s，作业未入队".formatted(handlerType);
        }
    }
}
