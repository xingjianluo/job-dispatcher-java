package com.github.xingjianluo.jobdispatcherjava.tests;

import com.github.xingjianluo.jobdispatcherjava.config.Clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用受控时钟：以创建时的真实时间为基准，正常情况下随真实时间流逝；
 * {@link #advanceMillis} 额外向前跳跃，用于一次性越过较长的退避/租约窗口，
 * 这样后台轮询线程既能自然醒来，又不必真的等待完整退避时长。
 */
class MutableClock implements Clock {

    private final long baseRealMillis;
    private final AtomicLong offset = new AtomicLong();

    MutableClock(long startMillis) {
        // startMillis 仅作为事件时间轴的起点；判定“到期”依赖流逝，因此锚定真实时间
        this.baseRealMillis = System.currentTimeMillis() - startMillis;
    }

    @Override
    public long nowMillis() {
        return System.currentTimeMillis() - baseRealMillis + offset.get();
    }

    /** 在当前流逝基础上额外向前跳跃。 */
    void advanceMillis(long delta) {
        offset.addAndGet(delta);
    }
}
