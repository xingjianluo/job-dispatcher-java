package com.github.xingjianluo.jobdispatcherjava.job;

import java.time.Instant;

/**
 * 作业状态流转记录：按发生顺序追加，单调递增的 sequence 保证
 * 并发或连续多次流转时顺序可校验、不丢不重。
 *
 * @param sequence   全局单调递增序号（同一调度器内唯一）
 * @param jobId      作业标识
 * @param at         发生时间
 * @param fromStatus 流转前状态（提交入队时为 null）
 * @param toStatus   流转后状态（续期等非状态变更事件 from==to）
 * @param attempt    事件发生时的尝试次数
 * @param actor      触发方（执行方标识或 system）
 * @param rationale  判定依据（为什么发生这次流转）
 */
public record JobEvent(long sequence, String jobId, Instant at,
                       JobStatus fromStatus, JobStatus toStatus,
                       int attempt, String actor, String rationale) {
}
