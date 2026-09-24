package com.github.xingjianluo.jobdispatcherjava.config;

import com.github.xingjianluo.jobdispatcherjava.domain.RetryPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 调度相关 Bean 装配。 */
@Configuration
@EnableConfigurationProperties(DispatcherProperties.class)
public class DispatcherConfig {

    @Bean
    public Clock clock() {
        return Clock.SYSTEM;
    }

    @Bean
    public RetryPolicy retryPolicy(DispatcherProperties props) {
        return new RetryPolicy(props.maxAttempts(), props.initialBackoff(), props.maxBackoff());
    }
}
