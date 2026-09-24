package com.github.xingjianluo.jobdispatcherjava.store;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** 基于 LinkedHashMap 的线程安全进程内存储。 */
@Component
public class InMemoryJobStore implements JobStore {

    private final Map<String, JobRecord> byIdempotencyKey = new LinkedHashMap<>();

    @Override
    public synchronized Optional<JobRecord> findByIdempotencyKey(String key) {
        return Optional.ofNullable(byIdempotencyKey.get(key));
    }

    @Override
    public synchronized Optional<JobRecord> findByJobId(String jobId) {
        return byIdempotencyKey.values().stream()
                .filter(j -> j.getJobId().equals(jobId))
                .findAny();
    }

    @Override
    public synchronized JobRecord putIfAbsent(String idempotencyKey, Function<String, JobRecord> factory) {
        JobRecord existing = byIdempotencyKey.get(idempotencyKey);
        if (existing != null) {
            return existing;
        }
        JobRecord created = factory.apply(idempotencyKey);
        // factory 可能涉及外部逻辑（虽然当前不会）；再校验一次，避免窗口期重复放入
        JobRecord raced = byIdempotencyKey.putIfAbsent(idempotencyKey, created);
        return raced != null ? raced : created;
    }

    @Override
    public synchronized Collection<JobRecord> findAll() {
        return List.copyOf(byIdempotencyKey.values());
    }

    @Override
    public synchronized int size() {
        return byIdempotencyKey.size();
    }
}
