package com.github.xingjianluo.jobdispatcherjava.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 对外 REST 入口。内部冲突异常在此转换为可解释的错误响应，
 * 底层实现异常不直接透出到对外结果中。
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobDispatcher dispatcher;

    public JobController(JobDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public record SubmitRequest(String idempotencyKey, int priority, String payload) {}
    public record AcquireRequest(String owner) {}
    public record LeaseActionRequest(String owner, String leaseId) {}
    public record FailRequest(String owner, String leaseId, boolean retryable, String message) {}

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = dispatcher.submit(request.idempotencyKey(), request.priority(), request.payload());
        return switch (result.outcome()) {
            case ACCEPTED -> ResponseEntity.status(HttpStatus.CREATED).body(toView(result.job()));
            case DUPLICATE -> ResponseEntity.ok(toView(result.job()));
            case REJECTED_QUEUE_FULL -> ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("outcome", result.outcome().name(), "reason", result.reason()));
        };
    }

    @PostMapping("/acquire")
    public ResponseEntity<?> acquire(@RequestBody AcquireRequest request) {
        AcquireResult result = dispatcher.acquire(request.owner());
        if (!result.acquired()) {
            return ResponseEntity.ok(Map.of("acquired", false, "reason", result.reason()));
        }
        return ResponseEntity.ok(Map.of(
                "acquired", true,
                "leaseId", result.leaseId(),
                "job", toView(result.job())));
    }

    @PostMapping("/{jobId}/renew")
    public Map<String, Object> renew(@PathVariable String jobId, @RequestBody LeaseActionRequest request) {
        dispatcher.renew(jobId, request.leaseId(), request.owner());
        return Map.of("renewed", true, "jobId", jobId);
    }

    @PostMapping("/{jobId}/complete")
    public Map<String, Object> complete(@PathVariable String jobId, @RequestBody LeaseActionRequest request) {
        dispatcher.complete(jobId, request.leaseId(), request.owner());
        return Map.of("completed", true, "jobId", jobId);
    }

    @PostMapping("/{jobId}/fail")
    public Map<String, Object> fail(@PathVariable String jobId, @RequestBody FailRequest request) {
        dispatcher.fail(jobId, request.leaseId(), request.owner(), request.retryable(), request.message());
        return Map.of("reported", true, "jobId", jobId);
    }

    @PostMapping("/{jobId}/cancel")
    public Map<String, Object> cancel(@PathVariable String jobId) {
        boolean effective = dispatcher.cancel(jobId);
        return Map.of("cancelled", effective, "jobId", jobId);
    }

    @PostMapping("/reap")
    public Map<String, Object> reap() {
        return Map.of("reaped", dispatcher.reapExpiredLeases());
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<?> get(@PathVariable String jobId) {
        return dispatcher.find(jobId)
                .<ResponseEntity<?>>map(job -> ResponseEntity.ok(toView(job)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "job not found", "jobId", jobId)));
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return dispatcher.listJobs().stream().map(JobController::toView).toList();
    }

    /** 内部冲突（租约丢失、状态非法、未知作业）转换为 409 与可解释原因。 */
    @ExceptionHandler(JobConflictException.class)
    public ResponseEntity<Map<String, Object>> conflict(JobConflictException ex) {
        log.warn("[conflict] {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "CONFLICT", "reason", ex.getMessage()));
    }

    /** 兜底：不向外部透出底层异常细节，仅返回通用错误并记录日志。 */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> unexpected(Exception ex) {
        log.error("[unexpected] internal error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "INTERNAL_ERROR", "reason", "internal error, see server logs"));
    }

    private static Map<String, Object> toView(Job job) {
        Map<String, Object> view = new java.util.LinkedHashMap<>();
        view.put("jobId", job.getId());
        view.put("idempotencyKey", job.getIdempotencyKey());
        view.put("priority", job.getPriority());
        view.put("status", job.getStatus().name());
        view.put("attempt", job.getAttempt());
        view.put("maxAttempts", job.getMaxAttempts());
        view.put("version", job.getVersion());
        view.put("failureKind", job.getFailureKind() == null ? "" : job.getFailureKind().name());
        view.put("failureMessage", job.getFailureMessage() == null ? "" : job.getFailureMessage());
        view.put("leaseOwner", job.getLeaseOwner() == null ? "" : job.getLeaseOwner());
        view.put("leaseExpiresAt", job.getLeaseExpiresAt() == null ? "" : job.getLeaseExpiresAt().toString());
        view.put("createdAt", job.getCreatedAt() == null ? "" : job.getCreatedAt().toString());
        return view;
    }
}
