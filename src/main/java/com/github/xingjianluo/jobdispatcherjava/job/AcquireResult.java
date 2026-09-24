package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 租约获取结果。
 */
public record AcquireResult(boolean acquired, Job job, String leaseId, String reason) {

    public static AcquireResult acquired(Job job, String leaseId) {
        return new AcquireResult(true, job, leaseId, "lease granted");
    }

    public static AcquireResult empty(String reason) {
        return new AcquireResult(false, null, null, reason);
    }
}
