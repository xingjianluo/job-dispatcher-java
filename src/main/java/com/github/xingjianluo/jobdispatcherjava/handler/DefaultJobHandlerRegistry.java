package com.github.xingjianluo.jobdispatcherjava.handler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 基于 Spring 注入的处理器注册表。 */
@Component
public class DefaultJobHandlerRegistry implements JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public DefaultJobHandlerRegistry() {
    }

    @Autowired
    public DefaultJobHandlerRegistry(List<JobHandler> handlers) {
        handlers.forEach(this::register);
    }

    @Override
    public Optional<JobHandler> find(String handlerType) {
        return Optional.ofNullable(handlers.get(handlerType));
    }

    @Override
    public void register(JobHandler handler) {
        JobHandler previous = handlers.put(handler.type(), handler);
        if (previous != null && previous != handler) {
            throw new IllegalStateException("duplicate handler type: " + handler.type());
        }
    }

    @Override
    public void replace(JobHandler handler) {
        handlers.put(handler.type(), handler);
    }
}
