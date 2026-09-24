package com.github.xingjianluo.jobdispatcherjava.handler;

/** 可重试失败：瞬时错误、依赖抖动、协作式取消/失联等。 */
public class RetryableJobException extends RuntimeException {
    public RetryableJobException(String message) {
        super(message);
    }

    public RetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
