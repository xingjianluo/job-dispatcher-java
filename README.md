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
| POST | `/api/jobs/{jobId}/cancel` | 取消作业：返回 `cancelled`（旧字段）+ `outcome`/`reason`（新增三态判定） |
| POST | `/api/jobs/reap` | 回收到期租约（纳入有界重试，见下文） |
| GET | `/api/jobs/{jobId}` / `/api/jobs` | 查询单个/全部作业状态 |
| GET | `/api/jobs/{jobId}/history` | 查询作业状态流转记录（按发生顺序，含判定依据） |

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
- 执行方失联时，租约到期后由 `reapExpiredLeases`（或 `POST /api/jobs/reap`）回收，**纳入与失败重试同一套有界重试**：
  - 每次回收消耗一次尝试机会（获取租约时已计入 `attempt`），并按同一指数退避策略安排 `nextAttemptAt`，退避未到期不会被抢占——不会刚回收就立刻被抢走空转；
  - 反复失联导致尝试次数耗尽时，进入终态 `FAILED`，`failureKind=LOST_RETRIES_EXHAUSTED`，与 `RETRIES_EXHAUSTED`（可重试失败耗尽）、`NON_RETRYABLE`（不可重试失败）三种原因明确区分；不会无限循环，也不会永久卡死；
  - 回收与退避不突破并发容量与队列容量上限：退避中的作业不占执行容量，重新调度仍需通过 `acquire` 的容量检查，不会超发；
  - 迟到的旧执行方回执（`renew`/`complete`/`fail`）因租约不匹配被拒绝（`409 CONFLICT`），同一作业不会被执行两次。
- 过期/被抢占/被取消的租约再调用 `renew`/`complete`/`fail` 会被拒绝并抛出内部冲突异常，REST 层转换为 `409 CONFLICT` 与可解释原因——旧租约不得再生效，因此不会重复执行。

## 失败分类与有界重试

- 失败分为**可重试**与**不可重试**：
  - 可重试失败在尝试次数未耗尽时按指数退避重新入队：`backoff = retry-backoff-base * 2^(attempt-1)`（默认基数 200ms，封顶 30s），退避未到期不会被调度；
  - 不可重试失败直接进入终态 `FAILED`，`failureKind=NON_RETRYABLE`；
  - 重试耗尽（达到 `max-attempts`，默认 3）进入终态 `FAILED`，`failureKind=RETRIES_EXHAUSTED`，与不可重试失败可区分。
- 取消进入终态 `CANCELLED`，`failureKind=CANCELLED`。

## 取消结果的三态判定

取消接口返回三种可区分的结果（`CancelOutcome`，REST 响应新增 `outcome`/`reason` 字段，保留原 `cancelled` 布尔字段）：

- `CANCELLED`：本次取消生效，作业进入终态 `CANCELLED`（此时 `cancelled=true`）；
- `ALREADY_CANCELLED`：作业此前已被取消过，本次不产生任何变化；
- `ALREADY_TERMINAL`：作业已因其它原因进入终态（如 `SUCCEEDED` 或 `FAILED`），本次取消不生效。

对已经结束的作业做取消不会改变其原有状态与失败原因（`failureKind`/`failureMessage` 保持原值），调用方可通过 `outcome` 直接看出是哪一种。旧的 `cancel(jobId)` 布尔接口语义不变：仅当本次生效时返回 `true`。

## 状态流转记录

每个作业从提交到结束的状态流转都按发生顺序记录（`JobEvent`），可通过 `GET /api/jobs/{jobId}/history` 或 `dispatcher.history(jobId)` 查询。每条记录包含：

- `sequence`：全局单调递增序号，并发或连续多次流转时顺序可校验、不丢不重；
- `fromStatus`/`toStatus`：流转方向（提交入队时 `fromStatus` 为空；续期等非状态变更事件 `from==to==LEASED`）；
- `attempt`：事件发生时的尝试次数；`actor`：触发方（执行方标识或 `system`）；
- `rationale`：判定依据（如退避时长、回收的失联执行方、终态失败分类）。

记录覆盖：提交入队、被谁拿走、续期、失联回收（含退避安排）、可重试失败触发的重试、最终进入的终态及原因。所有记录在同一把锁内追加，顺序与作业最终状态一致。
- 异常不被静默吞掉：内部冲突以 `JobConflictException` 显式抛出并记录日志；对外结果只暴露分类与原因，不透出底层实现异常（兜底为 `500 INTERNAL_ERROR`，细节仅记录在服务端日志）。

## 状态机

```
QUEUED --acquire--> LEASED --complete--> SUCCEEDED (终态)
  |                  |
  |                  +--fail(retryable, 未耗尽)--> QUEUED (退避后到期再调度)
  |                  +--fail(不可重试/耗尽)-------> FAILED (终态, NON_RETRYABLE / RETRIES_EXHAUSTED)
  |                  +--租约到期被回收, 未耗尽-----> QUEUED (退避后到期再调度)
  |                  +--租约到期被回收, 已耗尽-----> FAILED (终态, LOST_RETRIES_EXHAUSTED)
  +--cancel--> CANCELLED (终态, 任意非终态可取消)
```

## 兼容性说明

本轮新增能力均向后兼容：

- 既有 Java 方法签名（`submit`/`acquire`/`renew`/`complete`/`fail`/`cancel`/`reapExpiredLeases`）与 REST 请求结构不变；
- REST 取消响应保留原 `cancelled` 布尔字段，仅新增 `outcome`/`reason`；作业视图字段不变；
- 行为变化仅一处：租约到期被回收的作业改为退避到期后才能被重新抢占（此前回收立即可抢占），这是本轮要求的语义强化；
- `FailureKind` 新增枚举值 `LOST_RETRIES_EXHAUSTED`，旧值含义不变。

## 本地验证

```bash
mvn test
# 只跑本轮新增用例
mvn test -Dtest=JobDispatcherOpsTest
```

测试日志含作业标识、状态流转与判定依据（`[transition]`/`[assert]`/`[history]`），可直接观察失联回收的退避安排、取消三态判定与流转记录顺序。

## 测试覆盖

`JobDispatcherTest` 覆盖：幂等去重、优先级与容量控制、并发抢占唯一租约、租约过期回收与旧租约失效、续期防回收、退避重试与最终成功、不可重试/重试耗尽两种终态区分、取消与重复取消判别、取消使租约失效，以及多作业多执行方并发下"每作业恰好完成一次"的压力验证。

`JobDispatcherOpsTest` 覆盖本轮新增能力：失联回收消耗尝试次数与退避门控（退避未到期不可被抢占）、反复失联进入 `LOST_RETRIES_EXHAUSTED` 终态且与另外两种失败分类可区分、失联退避不占执行容量不超发、取消三态判定（生效/已取消过/其它终态）及终态原因不被覆盖、单作业全生命周期流转记录、并发下流转记录不丢不重且顺序与最终状态一致。

测试日志打印作业标识、状态流转与判定依据，可用 `mvn test` 复现。
