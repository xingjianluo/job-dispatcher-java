package com.github.xingjianluo.jobdispatcherjava.api;

/** 状态流转事件视图。 */
public record StateEventView(long timeMillis, String from, String to, String reason) {
}
