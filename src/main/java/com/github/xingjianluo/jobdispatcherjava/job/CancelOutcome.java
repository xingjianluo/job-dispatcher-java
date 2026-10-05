package com.github.xingjianluo.jobdispatcherjava.job;

/**
 * 取消结果分类，使调用方能区分取消到底发生了什么。
 */
public enum CancelOutcome {
    /** 本次取消生效，作业进入终态 CANCELLED */
    CANCELLED,
    /** 作业此前已被取消过，本次不产生任何变化 */
    ALREADY_CANCELLED,
    /** 作业已因其它原因进入终态（如 SUCCEEDED/FAILED），本次不产生任何变化 */
    ALREADY_TERMINAL
}
