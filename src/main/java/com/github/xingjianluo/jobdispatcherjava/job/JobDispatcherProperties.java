package com.github.xingjianluo.jobdispatcherjava.job;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 调度器可调参数。
 */
@Component
@ConfigurationProperties(prefix = "job.dispatcher")
public class JobDispatcherProperties {

    /** 同时在执行（被租约持有）的最大作业数 */
    private int maxConcurrent = 4;

    /** 队列（含执行中）最大容量，超出拒绝提交 */
    private int maxQueueCapacity = 100;

    /** 租约时长 */
    private Duration leaseDuration = Duration.ofSeconds(30);

    /** 默认最大尝试次数（含首次） */
    private int maxAttempts = 3;

    /** 重试退避基数，实际退避 = base * 2^(attempt-1) */
    private Duration retryBackoffBase = Duration.ofMillis(200);

    public int getMaxConcurrent() { return maxConcurrent; }
    public void setMaxConcurrent(int maxConcurrent) { this.maxConcurrent = maxConcurrent; }
    public int getMaxQueueCapacity() { return maxQueueCapacity; }
    public void setMaxQueueCapacity(int maxQueueCapacity) { this.maxQueueCapacity = maxQueueCapacity; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getRetryBackoffBase() { return retryBackoffBase; }
    public void setRetryBackoffBase(Duration retryBackoffBase) { this.retryBackoffBase = retryBackoffBase; }
}
