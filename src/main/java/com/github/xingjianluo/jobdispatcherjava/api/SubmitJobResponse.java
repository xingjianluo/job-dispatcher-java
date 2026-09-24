package com.github.xingjianluo.jobdispatcherjava.api;

/**
 * 提交响应。HTTP 200 表示请求被正常裁决（accepted=false 时由 reasonCode 解释）；
 * HTTP 4xx/5xx 见 {@code ApiError}。
 */
public record SubmitJobResponse(boolean accepted,
                                String reasonCode,
                                String reason,
                                JobView job,
                                Integer queuePosition,
                                Integer queueSize,
                                Integer activeLeases,
                                Integer capacity) {
}
