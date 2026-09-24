package com.github.xingjianluo.jobdispatcherjava.api;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;

import java.util.List;

/** 作业对外视图：状态、失败分类、租约归属与完整事件流均可观测。 */
public record JobView(String jobId,
                      String idempotencyKey,
                      String handlerType,
                      int priority,
                      String status,
                      String failureType,
                      String failureReason,
                      int attempts,
                      int maxAttempts,
                      String leaseOwner,
                      Long leaseExpiresAt,
                      long readyAt,
                      String result,
                      List<StateEventView> events) {

    public static JobView from(JobRecord job, int maxAttempts) {
        List<StateEventView> events = job.getEvents().stream()
                .map(e -> new StateEventView(e.timeMillis(),
                        e.from() == null ? null : e.from().name(),
                        e.to().name(), e.reason()))
                .toList();
        return new JobView(
                job.getJobId(),
                job.getIdempotencyKey(),
                job.getHandlerType(),
                job.getPriority(),
                job.getStatus().name(),
                job.getFailureType().name(),
                job.getFailureMessage(),
                job.getAttempts(),
                maxAttempts,
                job.getStatus() == com.github.xingjianluo.jobdispatcherjava.domain.JobStatus.LEASED
                        ? job.getLeaseOwner() : null,
                job.getStatus() == com.github.xingjianluo.jobdispatcherjava.domain.JobStatus.LEASED
                        ? job.getLeaseExpiresAt() : null,
                job.getReadyAt(),
                job.getResultPayload(),
                events);
    }
}
