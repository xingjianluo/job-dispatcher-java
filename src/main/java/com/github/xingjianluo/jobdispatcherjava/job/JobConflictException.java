package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 内部并发/状态冲突异常（如租约不匹配、状态非法）。
 * 仅用于服务内部，对外接口会转换为可解释的结果而非直接透出。
 */
public class JobConflictException extends RuntimeException {

    public JobConflictException(String message) {
        super(message);
    }
}
