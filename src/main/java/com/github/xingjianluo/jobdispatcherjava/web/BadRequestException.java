package com.github.xingjianluo.jobdispatcherjava.web;

/** 请求参数不合法。 */
public class BadRequestException extends RuntimeException {

    private final String errorCode;

    public BadRequestException(String message) {
        this("BAD_REQUEST", message);
    }

    public BadRequestException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public String errorCode() {
        return errorCode;
    }
}
