package com.github.xingjianluo.jobdispatcherjava.config;

/** 受控时钟，便于测试中驱动退避与租约过期。 */
public interface Clock {

    long nowMillis();

    /** 生产环境基于系统时间的时钟。 */
    Clock SYSTEM = System::currentTimeMillis;
}
