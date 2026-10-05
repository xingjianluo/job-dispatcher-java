package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 取消结果分类：让调用方能区分这次取消到底发生了什么。
 */
public enum CancelOutcome {
    /** 本次取消生效，作业进入终态 CANCELLED */
    CANCELLED,
    /** 作业此前已被取消过，本次不产生任何变化 */
    ALREADY_CANCELLED,
    /** 作业已因其它原因进入终态（成功/失败），本次取消不生效 */
    ALREADY_TERMINAL
}
