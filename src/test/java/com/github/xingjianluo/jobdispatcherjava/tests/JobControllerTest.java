package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.Clock;
import com.github.xingjianluo.jobdispatcherjava.handler.EchoJobHandler;
import com.github.xingjianluo.jobdispatcherjava.web.GlobalExceptionHandler;
import com.github.xingjianluo.jobdispatcherjava.web.JobController;
import com.github.xingjianluo.jobdispatcherjava.config.DispatcherProperties;
import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import com.github.xingjianluo.jobdispatcherjava.handler.DefaultJobHandlerRegistry;
import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import com.github.xingjianluo.jobdispatcherjava.service.JobScheduler;
import com.github.xingjianluo.jobdispatcherjava.store.InMemoryJobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Duration;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** 对外 HTTP 入口测试：原因码、状态码、错误脱敏与可观测视图。 */
class JobControllerTest {

    private static final Logger log = LoggerFactory.getLogger(JobControllerTest.class);

    private MockMvc mvc;
    private JobScheduler scheduler;
    private InMemoryJobStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        DefaultJobHandlerRegistry registry = new DefaultJobHandlerRegistry(java.util.List.of(new EchoJobHandler()));
        DispatcherProperties props = DispatcherProperties.defaults();
        RetryPolicy retry = new RetryPolicy(props.maxAttempts(), props.initialBackoff(), props.maxBackoff());
        scheduler = JobScheduler.forTesting(store, registry, props, retry, Clock.SYSTEM);
        scheduler.start();
        mvc = MockMvcBuilders.standaloneSetup(new JobController(scheduler, props))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("提交->查询->重复提交：200 + 判别原因码，视图含完整状态流转事件")
    void submitQueryAndDuplicate() throws Exception {
        String body = """
                {"idempotencyKey":"api-1","handlerType":"echo","payload":"hello","priority":5}
                """;
        MvcResult first = mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.reasonCode").value("ACCEPTED"))
                .andReturn();
        log.info("submit#1 response={}", first.getResponse().getContentAsString());

        // 等作业执行完
        String jobId = waitForSuccess("api-1");

        mvc.perform(get("/api/jobs/" + jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                .andExpect(jsonPath("$.failureType").value("NONE"))
                .andExpect(jsonPath("$.result").value("echo:hello"))
                .andExpect(jsonPath("$.events.length()").value(3));

        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reasonCode").value("DUPLICATE_SUBMISSION"))
                .andExpect(jsonPath("$.job.status").value("SUCCEEDED"));

        mvc.perform(get("/api/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    @DisplayName("参数校验与错误脱敏：400 BAD_REQUEST、404 JOB_NOT_FOUND，错误体为统一结构")
    void errorResponsesAreSanitized() throws Exception {
        MvcResult bad = mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"handlerType\":\"echo\"}"))
                .andExpect(status().isBadRequest())
                .andReturn();
        log.info("bad-request response={}", bad.getResponse().getContentAsString());
        // 错误体始终是统一 ApiError 结构，且不含内部实现细节
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"api-x\",\"handlerType\":\"missing-type\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("UNKNOWN_HANDLER"));

        mvc.perform(get("/api/jobs/no-such-id"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("JOB_NOT_FOUND"));

        // 取消不存在的作业 -> 404
        mvc.perform(post("/api/jobs/no-such-id/cancel"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("JOB_NOT_FOUND"));
    }

    @Test
    @DisplayName("不可重试失败的作业对外可区分 FAILED + NON_RETRYABLE；终态取消返回 409")
    void fatalFailureAndTerminalCancel() throws Exception {
        String body = """
                {"idempotencyKey":"api-fatal","handlerType":"echo","payload":"fail:fatal:永久错误"}
                """;
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk());

        String jobId = waitForTerminal("api-fatal");
        mvc.perform(get("/api/jobs/" + jobId))
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.failureType").value("NON_RETRYABLE"))
                .andExpect(jsonPath("$.failureReason").value("永久错误"));

        mvc.perform(post("/api/jobs/" + jobId + "/cancel"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reasonCode").value("TERMINAL_STATE"))
                .andExpect(jsonPath("$.cancelled").value(false));
    }

    private String waitForSuccess(String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            for (JobRecord j : store.findAll()) {
                if (j.getIdempotencyKey().equals(key) && "SUCCEEDED".equals(j.getStatus().name())) {
                    return j.getJobId();
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("作业未成功: " + key);
    }

    private String waitForTerminal(String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            for (JobRecord j : store.findAll()) {
                if (j.getIdempotencyKey().equals(key) && j.getStatus().isTerminal()) {
                    return j.getJobId();
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("作业未进入终态: " + key);
    }
}
