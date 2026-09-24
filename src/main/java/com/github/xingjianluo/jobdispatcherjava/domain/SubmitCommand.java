package com.github.xingjianluo.jobdispatcherjava.domain;

import java.util.Map;

/**
 * 提交作业的命令。
 *
 * @param idempotencyKey 幂等标识，同一标识全局只产生一次执行
 * @param handlerType    处理器类型，用于路由到对应 {@code JobHandler}
 * @param payload        处理器输入负载
 * @param priority       优先级，数值越大越优先；相同优先级按提交顺序
 */
public record SubmitCommand(String idempotencyKey,
                            String handlerType,
                            String payload,
                            Map<String, String> attributes,
                            int priority) {
}
