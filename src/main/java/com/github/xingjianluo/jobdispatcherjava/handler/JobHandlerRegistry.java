package com.github.xingjianluo.jobdispatcherjava.handler;

import java.util.Optional;

/** 处理器注册表：按 handlerType 路由。 */
public interface JobHandlerRegistry {

    Optional<JobHandler> find(String handlerType);

    /** 供测试注册自定义处理器。 */
    void register(JobHandler handler);

    /** 供测试替换同类型处理器（模拟故障执行方被健康执行方接替）。 */
    void replace(JobHandler handler);
}
