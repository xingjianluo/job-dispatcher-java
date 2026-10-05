package com.github.xingjianluo.jobdispatcherjava.job;

import java.time.Instant;

/**
 * 一条作业状态流转记录。
 *
 * @param sequence 全局单调递增序号（同一调度器内全序，并发下不丢不重）
 * @param jobId    作业标识
 * @param from     流转前状态；入队记录为 null
 * @param to       流转后状态
 * @param version  流转后的作业版本号（同一作业内从 0 起连续递增，可用于校验不丢不重）
 * @param at       流转发生时间
 * @param rationale 判定依据（谁触发、为什么这样流转）
 */
public record JobTransition(long sequence, String jobId, JobStatus from, JobStatus to,
                            long version, Instant at, String rationale) {
}
