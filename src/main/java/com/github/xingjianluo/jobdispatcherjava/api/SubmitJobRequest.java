package com.github.xingjianluo.jobdispatcherjava.api;

import java.util.Map;

/** 提交作业请求体。 */
public record SubmitJobRequest(String idempotencyKey,
                               String handlerType,
                               String payload,
                               Map<String, String> attributes,
                               Integer priority) {

    public int priorityOrDefault() {
        return priority == null ? 0 : priority;
    }
}
