# job-dispatcher-java

本地作业调度与执行服务。在**单进程内**模拟集群中的任务分发：用一把调度锁串行化所有作业状态流转，
用**租约 + 代际（epoch）**模拟执行方抢占，使并发提交、容量控制、租约过期与失败重试在
REST 入口、调度器入口下得到一致且可解释的结论。

- 技术栈：Java 21、Spring Boot 3.4（spring-boot-starter-web）、JUnit 5 / MockMvc
- 无外部服务、无数据库、无凭据：全部状态保存在进程内
- 幂等标识全局去重（含终态），重复提交永不产生第二次执行

---

## 1. 构建与本地验证

```bash
# 编译
mvn compile

# 全部单元/集成测试（19 个用例，含并发抢占与租约过期边界）
mvn test

# 启动服务（默认端口 8080）
mvn spring-boot:run
# 或
mvn package && java -jar target/job-dispatcher-java-1.0-SNAPSHOT.jar
```

### 手工验证（curl）

```bash
# 提交（返回 accepted/reasonCode/job，以及排队位置、队列长度、在途租约/容量）
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"demo-1",
  "handlerType":"echo",
  "payload":"hello",
  "priority":5
}'

# 查询（含完整状态流转事件流、失败分类、租约归属）
curl -s localhost:8080/api/jobs/<jobId>

# 列表
curl -s localhost:8080/api/jobs

# 取消（200 CANCELLED / 200 ALREADY_CANCELLED / 409 TERMINAL_STATE）
curl -s -X POST localhost:8080/api/jobs/<jobId>/cancel
```

内置 `echo` 处理器的负载约定：

| payload 前缀        | 行为                         |
|---------------------|------------------------------|
| `fail:retryable:x`  | 模拟可重试失败（进入退避重试） |
| `fail:fatal:x`      | 模拟不可重试失败（直接终态）   |
| 其他                | 成功，结果为 `echo:<payload>`  |

---

## 2. HTTP 接口与裁决结果

### `POST /api/jobs` 提交

| 场景                | HTTP | `accepted` | reasonCode               | 含义 |
|---------------------|------|------------|--------------------------|------|
| 正常入队            | 200  | true       | `ACCEPTED`               | 含排队位置、队列长度、在途租约/容量 |
| 幂等键已存在（任意状态，含终态） | 200 | false | `DUPLICATE_SUBMISSION` | 复用既有作业，不产生新执行 |
| 等待队列已满        | 429  | false      | `QUEUE_FULL`             | 作业**未入队、不会执行** |
| handlerType 不存在  | 400  | false      | `UNKNOWN_HANDLER`        | 入队前拒绝，作业不创建 |
| 缺少必填字段        | 400  | -          | `BAD_REQUEST`            | 统一错误体 |
| 非法 JSON           | 400  | -          | `MALFORMED_REQUEST`      | 统一错误体 |

注意区分两种“满”：

- **执行容量占满**（在途租约数 = `max-concurrent`）：作业仍然 `ACCEPTED` 并排队，**不拒绝、不超发**；
- **等待队列超长**（QUEUED+WAITING_RETRY 数 = `max-queue-size`）：才返回 `QUEUE_FULL` 拒绝。

### `GET /api/jobs/{id}` / `GET /api/jobs`

返回作业视图：状态、失败类型与原因、尝试次数/上限、当前租约 owner 与到期时间、完整事件流。
不存在返回 404 `JOB_NOT_FOUND`。

### `POST /api/jobs/{id}/cancel`

| 当前情况              | HTTP | reasonCode          |
|-----------------------|------|---------------------|
| 排队中/退避中/执行中，本次取消生效 | 200 | `CANCELLED` |
| 作业此前已是 CANCELLED（重复取消） | 200 | `ALREADY_CANCELLED` |
| 作业已处于 SUCCEEDED/FAILED/EXHAUSTED | 409 | `TERMINAL_STATE` |
| 作业不存在            | 404  | `JOB_NOT_FOUND`     |

未预期异常一律返回 500 `INTERNAL_ERROR`，**底层异常类名与堆栈只写入服务端日志，不透出到响应体**。

---

## 3. 调度语义

### 3.1 状态机

```
QUEUED ──发放租约──▶ LEASED ──成功────────────────────▶ SUCCEEDED     （终态）
  │                   │
  │                   ├──不可重试失败──────────────────▶ FAILED        （终态，NON_RETRYABLE）
  │                   ├──可重试失败 / 租约过期──────────▶ WAITING_RETRY
  │                   │                                      │
  │                   │◀────退避到期，重新发放租约────────────┘
  │                   └──可重试失败且已达 max-attempts────▶ EXHAUSTED     （终态）
  │                                                          failureType =
  │                                                          RETRY_EXHAUSTED 或 LEASE_EXPIRED
  └────────────────────────── 取消 ──────────────────────▶ CANCELLED     （终态）
            LEASED / WAITING_RETRY 也可取消 ───────────────▶ CANCELLED
```

### 3.2 幂等提交

- 以 `idempotencyKey` 为唯一键，提交时“查重 → 队列容量校验 → 创建”在调度锁内原子完成；
- 锁外还有一层快速查重，高并发下仍以锁内结果为准；
- 终态作业（SUCCEEDED / FAILED / EXHAUSTED / CANCELLED）同样占用幂等键，**终态后重复提交不会重跑**；
- 并发提交同一键的结论是可判别的：恰有 1 个 `ACCEPTED`，其余全部 `DUPLICATE_SUBMISSION`。

### 3.3 调度顺序与容量

- 可调度集合 = `QUEUED` 全部 + 已到退避时刻的 `WAITING_RETRY`；
- 排序：**优先级数值降序 → 提交序号升序（同优先级 FIFO）**；
- 每轮扫描在调度锁内核验 `在途租约数 < max-concurrent` 才发放租约；工作池大小同样等于容量上限，
  双保险保证**不超发**；容量不足只排队，不丢弃；
- 提交后、回执后、取消后、租约回收后都会立即补触发一次调度，轮询间隔仅作兜底。

### 3.4 可观测性

每次状态流转都写入作业的事件流并打印日志，内容包含：

- 作业标识 `jobId`、幂等键；
- `from -> to` 状态、租约 owner 与 epoch；
- 判定依据（为什么排队/拒绝、为什么重试或耗尽、退避多久、租约何时到期）；
- 尝试次数 `attempt / maxAttempts` 与失败分类。

---

## 4. 执行租约与并发抢占

- 调度器发放租约时：`QUEUED/WAITING_RETRY → LEASED`，写入 owner、到期时间，
  并把 `leaseEpoch + 1`，尝试次数 `attempts + 1`；
- 同一作业**任意时刻至多一个 LEASED 持有者**：状态流转全部在单把调度锁内完成，
  抢占不可能把同一作业同时发给两个执行方；
- 执行方（`WorkerTask`）每 **租约时长 / 3** 发送一次心跳续期；
  - 心跳校验 `(status=LEASED, owner, epoch)` 三元组，任一不符即拒绝；
  - 心跳被拒（已被取消或租约易主）会中断执行线程，执行方必须停手；
- **租约过期**：扫描器发现 `leaseExpiresAt <= now`，判定执行方失联（Full GC / 进程卡死 / 网络分区），
  回收租约并按**一次可重试失败**计数：
  - 未达上限：`LEASED → WAITING_RETRY`（failureType=`LEASE_EXPIRED`），退避后可被**重新分配**；
  - 达到上限：`LEASED → EXHAUSTED`（failureType 仍为 `LEASE_EXPIRED`，与普通重试耗尽可机读区分）；
  - 因此失联不会让作业永久卡住；
- **迟到回执防护**：失联的原执行方即使之后“苏醒”并提交成功/失败回执，调度器按 epoch 校验后直接拒绝，
  只记录日志（`回执拒绝 ... 迟到或越代结果已丢弃`），不会污染新持有者或终态，杜绝重复执行。

### 4.1 取消与租约

- QUEUED / WAITING_RETRY 取消：直接进入 CANCELLED，永无执行机会（QUEUED 取消 attempts=0）；
- LEASED 取消：回收租约进入 CANCELLED；执行方下一次心跳/回执被 epoch 校验拒绝；
  取消协作式生效（处理器可通过 `JobContext.cancelled()` 轮询提前退出）；
- 重复取消返回 `ALREADY_CANCELLED`，对其他终态取消返回 `TERMINAL_STATE`，二者与首次取消可判别。

> 测试开关：作业属性 `dispatch.disable-heartbeat=true` 会让该次执行不发送任何心跳，
> 用于在秒级以下稳定复现“执行方进程直接死掉”的失联场景（仅测试用途）。

---

## 5. 失败分类与有界重试

| 来源 | 分类 | 处理 |
|------|------|------|
| 返回 `NON_RETRYABLE_FAILURE` 或抛 `NonRetryableJobException` | `NON_RETRYABLE` | 第 1 次尝试即 `FAILED`，不重试 |
| 返回 `RETRYABLE_FAILURE` | `RETRYABLE` | 退避后重试，达到上限 → `EXHAUSTED/RETRY_EXHAUSTED` |
| 抛 `NonRetryableJobException` 以外的**任意异常/错误**（处理器缺陷、NPE 等） | `RETRYABLE`（防御性归类） | 完整堆栈写日志；对外只保存规整说明，按可重试处理 |
| 执行方租约过期（失联） | `LEASE_EXPIRED` | 退避后重新分配；达到上限 → `EXHAUSTED/LEASE_EXPIRED` |
| 取消 | `CANCELLED` | 终态 CANCELLED |

- 有界：总执行次数（首次 + 重试）恰为 `job-dispatcher.max-attempts`，**不会无限重试**；
- 退避：`min(max-backoff, initial-backoff * 2^(attempt-1))` 叠加 [50%,100%] 抖动，避免重试同拍；
- 不吞异常：未预期异常以 WARN 完整记录堆栈；调度/扫描线程的异常被捕获记录，
  不会杀死后台循环；对外响应只给稳定错误码与脱敏信息。

---

## 6. 配置项（`src/main/resources/application.properties`）

| 配置 | 默认 | 说明 |
|------|------|------|
| `job-dispatcher.max-concurrent` | 4 | 最大在途租约（并发执行容量） |
| `job-dispatcher.max-queue-size` | 1000 | 等待队列容量，超限拒绝入队（容量占满不拒绝） |
| `job-dispatcher.lease-duration` | 30s | 租约时长，执行方每 1/3 时长心跳 |
| `job-dispatcher.max-attempts` | 3 | 最大执行次数（首次 + 重试） |
| `job-dispatcher.initial-backoff` | 2s | 首次重试基础退避 |
| `job-dispatcher.max-backoff` | 60s | 退避上限 |
| `job-dispatcher.poll-interval` | 100ms | 调度扫描兜底周期 |
| `job-dispatcher.sweep-interval` | 200ms | 过期租约扫描周期 |

---

## 7. 测试覆盖与关键用例

`src/test/java/.../tests`：

- `JobSchedulerTest`：幂等去重（终态后仍去重）、容量排队不超发不丢弃、优先级 + FIFO 顺序、
  QUEUE_FULL / UNKNOWN_HANDLER 拒绝原因；
- `RetryAndFailureTest`：不可重试直接 FAILED、可重试有界退避至 EXHAUSTED、退避窗口内不重试、
  退避后恢复成功、未预期异常防御性归类且不泄漏内部细节；
- `LeaseAndPreemptionTest`（重点边界）：
  - 32 线程并发提交同一幂等键，仅 1 个作业、仅执行 1 次；
  - 心跳保活：执行时长超过租约时长但持续心跳时不被重分配；
  - 失联过期：不心跳 → 租约过期回收 → 退避后重新分配给新执行方成功 → 原执行方迟到回执被拒绝；
  - 反复失联：过期按失败计数，3 次后进入可区分的 `EXHAUSTED/LEASE_EXPIRED`，不永久卡住；
- `CancelTest`：排队中取消 / 重复取消 / 终态取消三者可判别，执行中取消回收租约且拒绝迟到回执，
  退避中取消不再重试；
- `JobControllerTest`：HTTP 状态码、原因码、统一错误体、脱敏与事件流视图。

测试日志统一打印 `jobId`、`from -> to` 状态流转与判定依据，可用以下命令查看：

```bash
mvn test -Dtest=LeaseAndPreemptionTest
# 详细报告：target/surefire-reports/
```

---

## 8. 代码结构

```
domain/   JobStatus / FailureType / JobRecord / RetryPolicy / StateEvent / SubmitCommand
handler/  JobHandler 扩展点、JobContext、JobOutcome、分类异常、注册表、内置 EchoJobHandler
store/    JobStore 接口与 InMemoryJobStore（进程内、线程安全）
service/  JobScheduler（调度锁、租约发放/续期/回收、重试、取消）、WorkerTask（执行壳/心跳/epoch 回执）
api/      JobView 等对外 DTO
web/      JobController、GlobalExceptionHandler、统一错误体
config/   DispatcherProperties、Clock、Bean 装配
```
