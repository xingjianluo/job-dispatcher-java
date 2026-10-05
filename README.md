# job-dispatcher-java

本地作业调度与执行服务：在进程内用作业状态机模拟集群中的任务分发，使并发抢占、租约过期与失败重试在各入口（REST 与内部服务）下得到一致且可解释的结论。

技术栈：Spring Boot 3.4（Web）、Java 21、JUnit 5，无仅使用 JDK 与仓库已有依赖，无外部服务。

## 运行与验证

```bash
# 运行全部测试（含并发抢占与租约过期边界场景）
mvn test

# 本地启动服务
mvn spring-boot:run
```

启动后可调用 REST 入口（默认 `http://localhost:8080`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/jobs` | 提交作业：`{"idempotencyKey","priority","payload"}` |
| POST | `/api/jobs/acquire` | 执行方获取租约：`{"owner"}` |
| POST | `/api/jobs/{jobId}/renew` | 续期租约：`{"owner","leaseId"}` |
| POST | `/api/jobs/{jobId}/complete` | 报告成功：`{"owner","leaseId"}` |
| POST | `/api/jobs/{jobId}/fail` | 报告失败：`{"owner","leaseId","retryable","message"}` |
| POST | `/api/jobs/{jobId}/cancel` | 取消作业，返回 `cancelled`（是否生效）与 `outcome`（三态判定，见下文） |
| POST | `/api/jobs/reap` | 回收到期租约（纳入有界重试，见下文） |
| GET | `/api/jobs/{jobId}` / `/api/jobs` | 查询单个/全部作业状态 |
| GET | `/api/jobs/{jobId}/history` | 按时间顺序查询该作业的状态流转记录（每条含判定依据） |

## 调度语义

- **幂等提交**：同一 `idempotencyKey` 的重复提交返回既有作业（`DUPLICATE`），不会产生第二次执行。
- **优先级排序**：就绪队列按优先级降序、同优先级按提交时间升序调度。
- **容量控制**：
  - 队列容量（`job.dispatcher.max-queue-capacity`，默认 100）满时，新提交被拒绝并返回 `REJECTED_QUEUE_FULL` 及原因，不会静默丢弃；
  - 执行容量（`job.dispatcher.max-concurrent`，默认 4）满时，`acquire` 返回未获取及原因，作业留在队列中等待，不会超发。
- **可解释性**：每次提交判定、租约获取与状态流转都打印日志，包含 `jobId`、流转方向、版本号与判定依据（`reason`）。

## 租约规则

- 同一作业任意时刻至多被一个执行方持有租约：所有状态变更在同一把锁内完成，并发抢占下 `acquire` 只会授予一个执行方。
- 租约时长由 `job.dispatcher.lease-duration`（默认 30s）控制；持有方可通过 `renew` 续期。
- 执行方失联时，租约到期后由 `reapExpiredLeases`（或 `POST /api/jobs/reap`）回收，**回收纳入与失败重试相同的有界重试**：
  - 每次回收消耗一次尝试机会（租约授予时已计入 `attempt`）；
  - 未耗尽时按相同的指数退避安排下一次可被抢占的时间（`nextAttemptAt = now + backoff`），退避到期前不会被抢走空转；
  - 耗尽时进入终态 `FAILED` 且 `failureKind=LOST`，与 `RETRIES_EXHAUSTED`（可重试失败耗尽）、`NON_RETRYABLE`（不可重试失败）三种终态原因可区分；
  - 因此失联作业不会无限循环，也不会永久卡死；回收与重试全程不突破并发容量与队列容量上限。
- 过期/被抢占/被取消的租约再调用 `renew`/`complete`/`fail` 会被拒绝并抛出内部冲突异常，REST 层转换为 `409 CONFLICT` 与可解释原因——失联执行方的迟到回执不得再生效，因此不会重复执行。

## 失败分类与有界重试

- 失败分为**可重试**与**不可重试**：
  - 可重试失败在尝试次数未耗尽时按指数退避重新入队：`backoff = retry-backoff-base * 2^(attempt-1)`（默认基数 200ms，封顶 30s），退避未到期不会被调度；
  - 不可重试失败直接进入终态 `FAILED`，`failureKind=NON_RETRYABLE`；
  - 重试耗尽（达到 `max-attempts`，默认 3）进入终态 `FAILED`，`failureKind=RETRIES_EXHAUSTED`，与不可重试失败可区分。
- **取消结果三态可区分**（`cancelJob` / `POST /api/jobs/{jobId}/cancel` 的 `outcome` 字段）：
  - `CANCELLED`：本次取消生效，作业进入终态 `CANCELLED`，`failureKind=CANCELLED`；
  - `ALREADY_CANCELLED`：作业此前已被取消过，本次不产生任何变化；
  - `ALREADY_TERMINAL`：作业已因其它原因进入终态（如 `SUCCEEDED`、`FAILED`），本次不产生任何变化，原有状态与失败原因（`failureKind`/`failureMessage`）保持不变。
- 异常不被静默吞掉：内部冲突以 `JobConflictException` 显式抛出并记录日志；对外结果只暴露分类与原因，不透出底层实现异常（兜底为 `500 INTERNAL_ERROR`，细节仅记录在服务端日志）。

## 状态流转记录

- 每条作业从提交到结束的全部流转都按时间顺序记录：入队、被谁拿走（租约授予）、续期、因失联被回收、触发重试、进入终态及原因。
- 每条记录包含：全局序号 `sequence`、流转前后状态 `from`/`to`、作业版本号 `version`、发生时间 `at`、判定依据 `rationale`。
- 所有记录都在同一把锁内追加：并发抢占或连续多次流转时，记录顺序与作业最终状态严格一致，不丢记录、不重复记录（同一作业内 `version` 从 0 连续递增，`sequence` 全局唯一）。
- 查询方式：
  - REST：`GET /api/jobs/{jobId}/history` 返回 `{"jobId", "transitions": [...]}`；
  - 服务内：`JobDispatcher.history(jobId)` 返回 `List<JobTransition>`。

## 兼容性说明

本轮新增能力不影响既有调用方式与返回结构：

- `JobDispatcher.cancel(jobId)` 的 `boolean` 语义不变（`true`=本次生效），三态判定通过新方法 `cancelJob(jobId)` 提供；
- 取消 REST 响应保留原有 `cancelled`、`jobId` 字段，仅新增 `outcome`、`reason` 字段；
- 提交、获取租约、续期、完成、失败、查询的入参与返回结构均未改变；
- 唯一的行为收紧：失联回收后不再立即可被抢占，而是按退避窗口安排（与失败重试一致），且回收消耗尝试次数、耗尽后落入 `LOST` 终态。

## 本地验证

```bash
# 全部测试（含失联重试、三态取消、并发流转记录校验）
mvn test

# 只跑新增用例
mvn test -Dtest='JobDispatcherTest#reapConsumesAttemptAndBackoffDelaysReacquire+repeatedOwnerLossEndsInDistinguishableLostTerminalState+cancelDistinguishesEffectiveAlreadyCancelledAndAlreadyTerminal+transitionHistoryCoversLifecycleInOrder+transitionHistoryIsCompleteAndOrderedUnderConcurrency'
```

测试日志中 `[transition]` 行包含 `jobId`、流转方向、版本号、全局序号与判定依据，`[assert]` 行标注每个检查点的判定理由。

## 状态机

```
QUEUED --acquire--> LEASED --complete--> SUCCEEDED (终态)
  |                  |
  |                  +--fail(retryable, 未耗尽)--> QUEUED (退避后到期再调度)
  |                  +--fail(不可重试/耗尽)-------> FAILED (终态, NON_RETRYABLE / RETRIES_EXHAUSTED)
  |                  +--租约到期被回收(未耗尽)-----> QUEUED (消耗一次尝试, 退避后到期再调度)
  |                  +--租约到期被回收(已耗尽)-----> FAILED (终态, LOST)
  +--cancel--> CANCELLED (终态, 任意非终态可取消)
```

## 测试覆盖

`JobDispatcherTest` 覆盖：幂等去重、优先级与容量控制、并发抢占唯一租约、租约过期回收与旧租约失效、续期防回收、退避重试与最终成功、不可重试/重试耗尽两种终态区分、取消与重复取消判别、取消使租约失效、多作业多执行方并发下"每作业恰好完成一次"的压力验证，以及本轮新增：失联回收消耗尝试次数与退避后才可再被抢占、反复失联进入 `LOST` 终态且不再循环、取消三态判定（生效/已取消过/已其它终态且原状态不变）、单作业全生命周期流转记录、并发下流转记录不丢不重且顺序与最终状态一致。测试日志打印作业标识、状态流转与判定依据，可用 `mvn test` 复现。
