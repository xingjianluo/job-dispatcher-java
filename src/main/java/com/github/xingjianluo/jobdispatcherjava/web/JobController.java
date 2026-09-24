package com.github.xingjianluo.jobdispatcherjava.web;

import com.github.xingjianluo.jobdispatcherjava.api.JobView;
import com.github.xingjianluo.jobdispatcherjava.api.SubmitJobRequest;
import com.github.xingjianluo.jobdispatcherjava.api.SubmitJobResponse;
import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.SubmitCommand;
import com.github.xingjianluo.jobdispatcherjava.service.CancelResult;
import com.github.xingjianluo.jobdispatcherjava.service.JobScheduler;
import com.github.xingjianluo.jobdispatcherjava.service.SubmitResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 作业调度 REST 入口。 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobScheduler scheduler;
    private final DispatcherProperties props;

    public JobController(JobScheduler scheduler, DispatcherProperties props) {
        this.scheduler = scheduler;
        this.props = props;
    }

    @PostMapping
    public ResponseEntity<SubmitJobResponse> submit(@RequestBody SubmitJobRequest request) {
        validate(request);
        SubmitCommand command = new SubmitCommand(request.idempotencyKey(), request.handlerType(),
                request.payload(), request.attributes(), request.priorityOrDefault());
        SubmitResult result = scheduler.submit(command);
        return switch (result) {
            case SubmitResult.Accepted accepted -> ResponseEntity.ok(new SubmitJobResponse(
                    true, accepted.reasonCode(), accepted.reason(),
                    JobView.from(accepted.job(), props.maxAttempts()),
                    accepted.queuePosition(), accepted.queueSize(),
                    accepted.activeLeases(), accepted.capacity()));
            case SubmitResult.Duplicate duplicate -> ResponseEntity.ok(new SubmitJobResponse(
                    false, duplicate.reasonCode(), duplicate.reason(),
                    JobView.from(duplicate.job(), props.maxAttempts()),
                    null, null, null, props.maxConcurrent()));
            case SubmitResult.RejectedQueueFull full -> ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(new SubmitJobResponse(
                            false, full.reasonCode(), full.reason(),
                            JobView.from(full.job(), props.maxAttempts()),
                            null, full.queueSize(), null, props.maxConcurrent()));
            case SubmitResult.RejectedUnknownHandler unknown ->
                    // 入队前的非法请求走统一错误体（不产生作业实体，不返回作业视图）
                    throw new BadRequestException("UNKNOWN_HANDLER", unknown.reason());
        };
    }

    @GetMapping("/{jobId}")
    public JobView get(@PathVariable String jobId) {
        return scheduler.get(jobId)
                .map(j -> JobView.from(j, props.maxAttempts()))
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    @GetMapping
    public List<JobView> list() {
        return scheduler.list().stream()
                .map(j -> JobView.from(j, props.maxAttempts()))
                .toList();
    }

    @PostMapping("/{jobId}/cancel")
    public ResponseEntity<CancelResponse> cancel(@PathVariable String jobId) {
        CancelResult result = scheduler.cancel(jobId);
        JobView view = JobView.from(result.job(), props.maxAttempts());
        return switch (result) {
            case CancelResult.Cancelled c ->
                    ResponseEntity.ok(new CancelResponse(true, "CANCELLED", c.reason(), view));
            case CancelResult.AlreadyCancelled c ->
                    ResponseEntity.ok(new CancelResponse(false, "ALREADY_CANCELLED", c.reason(), view));
            case CancelResult.Terminal c ->
                    ResponseEntity.status(HttpStatus.CONFLICT)
                            .body(new CancelResponse(false, "TERMINAL_STATE", c.reason(), view));
        };
    }

    private void validate(SubmitJobRequest request) {
        if (request == null) {
            throw new BadRequestException("请求体不能为空");
        }
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new BadRequestException("idempotencyKey 不能为空");
        }
        if (request.handlerType() == null || request.handlerType().isBlank()) {
            throw new BadRequestException("handlerType 不能为空");
        }
    }

    /** 取消响应。 */
    public record CancelResponse(boolean cancelled, String reasonCode, String reason, JobView job) {
    }
}
