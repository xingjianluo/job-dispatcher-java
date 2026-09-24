package com.github.xingjianluo.jobdispatcherjava.domain;

/**
 * 作业状态流转事件（可观测性依据）。
 *
 * @param timeMillis 发生时间（受控时钟）
 * @param from       原状态（首次入队时为 null）
 * @param to         新状态
 * @param reason     判定依据说明
 */
public record StateEvent(long timeMillis, JobStatus from, JobStatus to, String reason) {
}
