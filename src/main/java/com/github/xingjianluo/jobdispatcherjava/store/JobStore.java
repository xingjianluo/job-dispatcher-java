package com.github.xingjianluo.jobdispatcherjava.store;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * 进程内作业存储。模拟集群中共享的作业表：
 * 单进程内用全局监视器保证状态流转原子性，避免并发抢占造成重复发放/重复执行。
 */
public interface JobStore {

    /** 按幂等键查找（任何状态，包括终态，保证重复提交永不二次执行）。 */
    Optional<JobRecord> findByIdempotencyKey(String key);

    /** 按作业标识查找。 */
    Optional<JobRecord> findByJobId(String jobId);

    /**
     * 幂等放入：键不存在时用 factory 创建并保存，返回保存后的记录；
     * 键已存在时直接返回既有记录，factory 不会被调用。
     */
    JobRecord putIfAbsent(String idempotencyKey, Function<String, JobRecord> factory);

    /** 全部作业（含终态），供扫描与可观测查询。 */
    Collection<JobRecord> findAll();

    /** 当前存储中的作业数量。 */
    int size();
}
