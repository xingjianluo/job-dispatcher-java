package com.github.xingjianluo.jobdispatcherjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内置示例处理器：原样返回负载，便于本地手工验证。
 * 负载以 "fail:retryable:" 开头时模拟可重试失败；"fail:fatal:" 开头模拟不可重试失败。
 */
@Component
public class EchoJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(EchoJobHandler.class);

    public static final String TYPE = "echo";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public JobOutcome handle(String payload, JobContext context) {
        log.info("jobId={} attempt={} epoch={} handler=echo 开始执行 payload={}",
                context.jobId(), context.attempt(), context.leaseEpoch(), payload);
        if (payload != null && payload.startsWith("fail:fatal:")) {
            String reason = payload.substring("fail:fatal:".length());
            log.info("jobId={} attempt={} 判定为不可重试失败: {}",
                    context.jobId(), context.attempt(), reason);
            return JobOutcome.nonRetryableFailure(reason);
        }
        if (payload != null && payload.startsWith("fail:retryable:")) {
            String reason = payload.substring("fail:retryable:".length());
            log.info("jobId={} attempt={} 判定为可重试失败: {}",
                    context.jobId(), context.attempt(), reason);
            return JobOutcome.retryableFailure(reason);
        }
        String echo = payload == null ? "" : payload;
        log.info("jobId={} attempt={} 执行成功", context.jobId(), context.attempt());
        return JobOutcome.success("echo:" + echo);
    }
}
