package com.github.xingjianluo.jobdispatcherjava.web;

/** 请求的作业不存在。 */
public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("job not found: " + jobId);
    }
}
