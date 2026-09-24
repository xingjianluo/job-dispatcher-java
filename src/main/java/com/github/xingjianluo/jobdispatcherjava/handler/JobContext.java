package com.github.xingjianluo.jobdispatcherjava.handler;

/**
 * 执行方处理作业时拿到的上下文。
 */
public interface JobContext {

    /** 作业标识。 */
    String jobId();

    /** 当前执行次数（从 1 开始）。 */
    int attempt();

    /** 当前租约代际。 */
    long leaseEpoch();

    /**
     * 作业是否被请求取消（取消为协作式：处理器可在长任务中轮询）。
     * 执行方被取消或失去租约后应尽快抛出
     * {@link com.github.xingjianluo.jobdispatcherjava.handler.RetryableJobException} 或提前退出。
     */
    boolean cancelled();
}
