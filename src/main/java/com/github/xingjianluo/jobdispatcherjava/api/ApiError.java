package com.github.xingjianluo.jobdispatcherjava.api;

import java.time.Instant;

/** 统一错误体：不透出底层实现异常，只给稳定错误码与脱敏说明。 */
public record ApiError(Instant timestamp, int status, String errorCode, String message) {
}
