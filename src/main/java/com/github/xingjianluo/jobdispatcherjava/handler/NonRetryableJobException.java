package com.github.xingjianluo.jobdispatcherjava.handler;

/** 不可重试失败：参数非法、业务永久拒绝等。 */
public class NonRetryableJobException extends RuntimeException {
    public NonRetryableJobException(String message) {
        super(message);
    }

    public NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
