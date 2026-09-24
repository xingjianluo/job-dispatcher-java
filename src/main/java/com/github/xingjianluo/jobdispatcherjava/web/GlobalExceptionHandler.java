package com.github.xingjianluo.jobdispatcherjava.web;

import com.github.xingjianluo.jobdispatcherjava.api.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * 统一异常处理：底层异常不直接透出，对外只返回稳定错误码与脱敏说明；
 * 详细堆栈仅记录在服务端日志中（不静默吞掉）。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(BadRequestException.class)
    public ResponseEntity<ApiError> handleBadRequest(BadRequestException ex) {
        // 参数错误可预期，info 即可；说明本身就是面向调用方的脱敏信息
        log.info("请求参数不合法: code={} message={}", ex.errorCode(), ex.getMessage());
        return error(HttpStatus.BAD_REQUEST, ex.errorCode(), ex.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadable(HttpMessageNotReadableException ex) {
        log.info("请求体无法解析: {}", ex.getMessage());
        return error(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST", "请求体不是合法 JSON 或字段类型不匹配");
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(JobNotFoundException ex) {
        return error(HttpStatus.NOT_FOUND, "JOB_NOT_FOUND", "作业不存在或已不可查");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex) {
        // 完整堆栈只进日志；对外只给通用错误码，绝不透出底层实现异常
        log.error("未预期的服务端异常", ex);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "服务内部错误，请稍后重试");
    }

    private ResponseEntity<ApiError> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status)
                .body(new ApiError(Instant.now(), status.value(), code, message));
    }
}
