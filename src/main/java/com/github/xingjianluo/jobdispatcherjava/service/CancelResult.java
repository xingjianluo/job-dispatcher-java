package com.github.xingjianluo.jobdispatcherjava.service;

import com.github.xingjianluo.jobdispatcherjava.domain.JobRecord;

/**
 * 取消入口的判别式结果；重复取消与对终态作业取消必须可区分。
 */
public sealed interface CancelResult {

    JobRecord job();

    String reason();

    /** 本次请求将非终态作业转为 CANCELLED（排队中/退避中直接取消；租约中协作中断）。 */
    record Cancelled(JobRecord job) implements CancelResult {
        @Override public String reason() {
            return "作业已被取消，状态流转为 CANCELLED（取消前状态见事件流）";
        }
    }

    /** 作业已经是 CANCELLED：重复取消，不产生新流转。 */
    record AlreadyCancelled(JobRecord job) implements CancelResult {
        @Override public String reason() {
            return "作业此前已取消，重复取消被忽略";
        }
    }

    /** 作业已处于其它终态（成功/失败/耗尽），不可取消。 */
    record Terminal(JobRecord job) implements CancelResult {
        @Override public String reason() {
            return "作业已处于终态 %s，无法取消".formatted(job().getStatus());
        }
    }
}
