package com.github.xingjianluo.jobdispatcherjava.handler;

/** 作业处理器：执行方实际业务逻辑的扩展点。 */
public interface JobHandler {

    /** 处理器类型标识，对应提交命令中的 handlerType。 */
    String type();

    /** 执行一次尝试。抛异常等价于可重试失败（除非是 {@link NonRetryableJobException}）。 */
    JobOutcome handle(String payload, JobContext context) throws Exception;
}
