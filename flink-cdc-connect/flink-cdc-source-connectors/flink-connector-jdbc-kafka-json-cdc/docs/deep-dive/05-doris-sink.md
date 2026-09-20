<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Doris Sink：StreamLoad 写入与 DDL 执行（全 HTTP，自包含）

> 本文讲连接器**完全自包含**的 Doris 写入：不引入 doris connector jar、不引入 mysql-jdbc，全部交互走 HTTP。
> 写数据用 StreamLoad PUT，执行 DDL 用 FE 的 HTTP 查询接口。
> 关联：[04-ddl-blocking.md](./04-ddl-blocking.md)（阻塞协议的数据落点）、[03-event-model.md](./03-event-model.md)（10 种事件的消费）。
> 代码位置：`sink/engine/doris/`（pipeline 模块）。

---

## 1. 为什么全 HTTP、为什么 OkHttp

| 约束 | 原因 |
|---|---|
| 不引入 doris connector jar | 用户拍板：连接器自包含，Doris 交互全走 HTTP，避免外部 jar 的版本/依赖风险 |
| 不引入 mysql-jdbc | DDL 执行走 FE HTTP 查询接口（`is_execute_sql_in_http=true`），不需要 JDBC |
| HTTP 客户端用 OkHttp 3.14.9 | 纯 Java、无 kotlin 依赖、自带连接池与重定向/重试控制，shade 进连接器 jar |

> 注：实现阶段把计划文件里「引入 doris jar 思路」的替代做成了唯一路径——两个 HTTP 端点见 §2。

---

## 2. 两个 HTTP 端点

| 端点 | 用途 | 说明 |
|---|---|---|
| `PUT /api/{db}/{table}/_stream_load` | StreamLoad 批量写 | 见 §3 |
| `POST /api/query/default_cluster/{db}`（body `{"stmt": ...}`） | DDL 执行 | 要求 FE 开 `is_execute_sql_in_http=true`（默认 false，见 §7 风险） |

> 早期计划文件写的 body 是 `{"sql":...}`，**实际实现是 `{"stmt":...}`**（与 Doris FE HTTP 查询接口一致）。

---

## 3. StreamLoad 写入（非 2PC）

### 3.1 类与职责

本节讲**默认档（`sink.writer=legacy`）**的写入路径；三档的分工与另外两档见 §4。

```
DorisSink implements Sink<Event>（非 TwoPhaseCommittingSink）
  └─ createWriter → DorisSinkWriter implements SinkWriter<Event>
       ├─ 本地 schema 视图（schemaMaps：TableId → Schema；rowConverters：TableId → DorisRowConverter）
       ├─ 每表 FIFO 缓冲（buffer：TableId → ArrayDeque<Map<String,Object>>，upsert/delete 同队列保序）
       └─ DorisHttpClient（OkHttp）
```

**为什么是"非 2PC"**：`Sink` 不是 `TwoPhaseCommittingSink` → 没有 precommit/commit 阶段，
**一次 StreamLoad PUT 就是一次提交**（at-least-once）。要两阶段就用 `sink.writer=stateful-2pc`
（§4），legacy 这一档**一行不改**，作为回归基线。

> **更正（2026-09-20，本次审计）**：此前记过一句"legacy 无状态 ⇒ 被 checkpoint 覆盖的缓冲行会丢"，
> **这是错的**。Flink 1.18.1 的 `SinkWriterOperator.prepareSnapshotPreBarrier` 在**每个 checkpoint
> barrier 之前无条件调 `sinkWriter.flush(false)`**，而这里的 `flush(boolean)` 就是同步 PUT；所以
> 被某个已完成 checkpoint 覆盖的每一行，在它完成之前就已经在 Doris 里。**legacy 的 1PC 路径没有
> 丢数洞**。§4 改变的是别的东西：可见性边界与跨重启的单调性。

### 3.2 触发 StreamLoad 的时机

1. **单表缓冲达到 `sink.buffer.size`**（每表阈值）；
2. **全局有界缓存**：总缓冲行数超过 `sink.buffer.max-buffered-rows` 时，**spill 最大的表队列**
   （防止跟踪很多小表时内存无界增长）；
3. **周期 flush 定时器**（`sink.flush.interval`，为 0/负则关闭）；
4. **每个 `FlushEvent`**：`DataSinkWriterOperator` 拦截 FlushEvent 直接调 `writer.flush()`（**强制 PUT**），
   这就是 [04-ddl-blocking.md](./04-ddl-blocking.md) 阻塞协议里"等所有并行度 flush 完"的数据落点；
5. **close()**：最终 PUT。

### 3.3 StreamLoad 请求细节（`DorisHttpClient.streamLoad`）

```
PUT {fe}/api/{db}/{table}/_stream_load
headers: label      = cdc_{db}_{table}_{uuid}
         format     = json
         strip_outer_array = true
         hidden_columns    = __DORIS_DELETE_SIGN__   ← delete 标记列（§3.4）
         Authorization = Basic(base64(user:pass))
         Expect       = 100-continue                ← FE StreamLoad handler 必需
body: JSON 数组（每元素是「列名 → JSON 值」的 Map）
```

**307 重定向两步走**：Doris 2.x 的 StreamLoad PUT 由 FE 答 `307`，`Location` 指向真正持有 tablet 的 BE。
OkHttp **不为 PUT 跟重定向**，所以客户端自己做两步：先问 FE（带 `Expect: 100-continue`），拿到 307 后
去掉 `Location` 里 FE 烘进去的 `user:pass@` 前缀，再对 BE 重发同一请求。

**失败重试是幂等的**：重试用**同一个 label**，Doris 按 label 去重。网络层失败按 `sink.max-retries` 重试
（`500ms * (attempt+1)` 退避）。

**成功判定**：HTTP 2xx 且响应体 `Status` 为 `Success` 或 `Label Already Exists`（后者等于"上次已成功"）。

### 3.4 DELETE 语义（Unique 模型 + 标记列）

- 每行都带 `__DORIS_DELETE_SIGN__` 标记：`true` = 按主键删除，`false` = upsert；
- 该列通过 `hidden_columns` header 声明，Doris 直接应用语义，**不需要 `merge_type`**；
- **建表不用 `enable_batch_delete_by_default`**（旧版批量删除开关，**Doris 2.x 已拒绝该属性**）——Unique 模型
  配合 `hidden_columns` 标记即可；
- DELETE 事件从**前像**（`event.before()`）构造行 + 标记 `true`；INSERT/UPDATE/REPLACE 都按 upsert
  （`event.after()` + 标记 `false`）；upsert 和 delete 共用一条队列，保证到达顺序。

> 早期计划文件里"建表属性 `enable_batch_delete_by_default=true`"的思路，实现阶段因 Doris 2.x 拒绝该属性而改为
> `hidden_columns` header。**以本实现为准。**

### 3.5 行转换

`KafkaJsonRowConverter`（抽象，`sink/converter/`）→ `DorisRowConverter`：
- `createExternalConverter(DataType)` 按 typeRoot 分派：数值/字符串原样、时间格式化（用 `pipelineZoneId`）、
  ARRAY/MAP/ROW → JSON 字符串；default 抛 `UnsupportedOperationException`；
- `convert(RecordData, Schema)` 产出「列名 → JSON 值」的 Map。

### 3.6 写入方如何演进 schema（`DorisSinkWriter.applySchemaChange`）

| 事件 | writer 本地行为 |
|---|---|
| `CreateTableEvent` | 注册 schema + converter |
| `RenameTableEvent` | **逐对换 key**（后续数据带新 id）：先把**所有** old key 的 schema/converter/缓冲读出来，再统一写 new key——`a→b, b→a` 的对调里就地搬会让第二对读回第一对刚写进去的东西（rename DDL 前阻塞协议已 flush，缓冲通常为空） |
| `DropTableEvent` | 丢弃该表缓冲 + schema + converter |
| `TruncateTableEvent` / 两个 Comment 事件 | **不动**（阻塞协议已先 flush；comment 不影响行转换） |
| 5 个标准 schema change | `SchemaUtils.applySchemaChangeEvent(current, event)` 演进 schema + 重建 converter |

> **坑**：`write(DataChangeEvent)` 时若 `schemaMaps` 里没有该表 → 抛 `IOException`
> （"CreateTableEvent must precede its data"）——**CreateTableEvent 必须早于数据到达**。

---

## 4. 状态与两阶段提交（`sink.writer`）

### 4.1 三档与它们的差异

| 档 | `sink.writer` 的值 | 类 | 写入语义 |
|---|---|---|---|
| legacy（默认） | `legacy` | `DorisSink` / `DorisSinkWriter` | 原样保留（§3）：一次 PUT 即一次提交，at-least-once + label 幂等 |
| 有状态 1PC | `stateful` | `StatefulDorisSink` / `StatefulDorisSinkWriter` | 写入行为**与 legacy 相同**（barrier 前全量 PUT），多一个可 checkpoint 的 sequenceCounter |
| 有状态 2PC | `stateful-2pc`（也接受 `stateful_2pc`） | `TwoPhaseDorisSink` / `TwoPhaseDorisSinkWriter` + `DorisCommitter` | 每批一次**预提交**，checkpoint 完成后由 committer 提交；**checkpoint 完成前行不可见** |

`DorisDataSinkDialect.createSink()` 按 `getWriterMode()` 分派；`getWriterMode()` 把配置值解析成
`WriterMode` 枚举（解析后的值只在**校验**里用，配置项本身是 `String`——枚举 ConfigOption 会被
Flink 把值大写化，`stateful-2pc` 就再也写不出来了）。

`KafkaJsonDataSinkBuilder` 只在 2PC 档做两件额外的事：writer transform 的类型从
`CommittableMessageTypeInfo.noOutput()` 换成 `of(sink::getCommittableSerializer)`，并在其后挂
`CommitterOperatorFactory<>(sink, false, true)`（终止分支，不作为返回值）。

**两个 sink 类而不是一个**：Flink 1.18.1 没有 `TwoPhaseCommittingStatefulSink` 合并接口（1.19+ 才有），
所以 2PC 档只能一个类同时实现 `StatefulSink` + `TwoPhaseCommittingSink`。算子用
`instanceof TwoPhaseCommittingSink` 决定是否发 committable，因此 **1PC 档绝不能实现它**——
否则会在 `noOutput()` 的 writer 流上发没人接的 committable。用两个类隔离，比运行时开关安全。

第三参数 `isCheckpointingEnabled` 固定传 `true`：它**只在 `endInput()` 里用**，为 false 时算子在
输入结束时会 `notifyCheckpointComplete(Long.MAX_VALUE)`，把没有 checkpoint 覆盖的事务提交掉。

### 4.2 状态（`DorisWriterState`）

只有两个字段：

| 字段 | 为什么 |
|---|---|
| `sequenceCounter` | Group Commit 的序号。恢复时取 `max(状态值 + 1, 墙钟基数)`：跨重启严格单调，不靠"时钟一定往前走"，也不怕时钟回拨 |
| `labelPrefix` | 状态是在哪个前缀下写的。**不参与任何判断**（挂起事务靠 label 冲突发现，不靠状态），只是让 savepoint 自描述：换个前缀恢复时日志里说得清 |

**状态里没有的两样东西，以及为什么**（这是与最初设计相比最大的偏离）：

- **没有 in-flight 事务列表**。在"要恢复的那个 checkpoint"里开着的事务，**正是 committer 算子马上要
  提交的那批**——它自己在状态里存着，而提交通知是异步的：作业完全可能在 checkpoint 完成之后、
  committable 提交之前死掉。若 writer 恢复时把它们 abort 掉，重启后的 committer 会撞上
  "transaction [N] is already aborted" 而**永远起不来**。而"checkpoint 没完成时开的事务"也不是孤儿：
  后一个 checkpoint 完成时 `getCheckpointCommittablesUpTo` 会把它们提交掉；真正无主的（表来自一个
  从未 checkpoint 的尝试）由 Doris 按 `stream_load_default_precommit_timeout_second`（3600s，F11）回收。
- **没有缓冲行**。取状态时缓冲必然是空的：每个 checkpoint barrier 之前 Flink 都会先 `flush(false)`（§3.1 的更正）。

### 4.3 label 的形状（2PC 档）

```
{labelPrefix}_{database}_{table}_{subtask}_{epoch}_{rung}
例：cdc_shop_orders_0_8_1
```

- `epoch` = **这批行所属的 checkpoint id**：恢复时 `getRestoredCheckpointId() + 1`，之后每次
  `snapshotState(id)` 置为 `id + 1`；新作业（无恢复点）是 `INITIAL_CHECKPOINT_ID`(=1)。
  epoch 必须严格递增：撞上**已经 commit** 的 label 不可恢复（F13），只能响亮失败。
- `rung` = 该 epoch 内该表的第几批（从 0 起）。一个 epoch 里的行不只有 checkpoint 那一批——
  缓冲阈值、周期定时器、FlushEvent 都会 flush（§3.2），每批都要自己的 label。

legacy/1PC 档的 label **仍然是随机 UUID**（§3.3）。那里没有事务边界，决定性 label 会因为
"同一 label、内容不同"被 Doris **静默跳过**而丢行；重复交给 UNIQUE KEY 的 upsert 吸收。

### 4.4 恢复：label 冲突就是入口

2PC 档的重试**不能**换一个 label（这正是它必须可推导的原因）。一个已经被占的 label 的处理方式：

| Doris 的回答 | 处理 |
|---|---|
| `ExistingJobStatus=PRECOMMITTED`（消息里带 `txn [N]`） | **abort N，然后用同一个 label 重载**；F12 实测：abort 后 label 被释放，同一 label 能再拿到新事务 |
| `ExistingJobStatus=FINISHED` | **抛错**：这批 label 已被别的东西用掉（共用前缀的另一作业、或比 label 更早的 savepoint），重试不会变好。错误消息里点名 `sink.label-prefix` |
| 消息里没有 txn id | **抛错**（"cannot clear"）：没有事务可 abort，重试只会掩盖问题 |

于是**恢复路径不需要查询 Doris**：重启的 writer 直接写回它上次用过的 label，Doris 报出占着它的
事务号，abort 掉再重载。这也是状态里不需要 in-flight 列表的原因（§4.2）。

### 4.5 两阶段协议（客户端侧）

| 调用 | 请求 | 判定 |
|---|---|---|
| 预提交 | `PUT /api/{db}/{table}/_stream_load` + 头 `two_phase_commit: true`（**两腿都要带**，FE 的 307 不是代理） | `Status=Success` + 响应里的 `TxnId`；没有 TxnId 直接抛错 |
| commit / abort | `PUT /api/{db}/_stream_load_2pc`（**路径里没有表名**）+ `txn_operation=commit\|abort` + `txn_id` | 见下 |

判定（`evaluateTwoPhaseOperation`）：

- `status=Success` → 成功；
- `already visible`（**大小写不敏感**，覆盖 commit 侧 `"already visible, not pre-committed."` 与
  abort 侧 `"already VISIBLE, could not abort."`）→ 当成功：行已发布，重放到这里就算做完了；
- abort 且 `transaction [N] not found` → 当成功：没东西可 abort（Doris 已回收）；
- `already aborted` → **抛错**：数据已被丢弃，吞掉等于静默丢数（commit 与 abort 都抛）。

`DorisCommitter.commit` 逐条发 commit，**失败抛异常而不是 signal**：Flink 1.18.1 里
`signalFailedWithKnownReason` 只把请求标记失败，`SubtaskCommittableManager.drainCommitted` 随后把它
丢掉并加一个指标（FLINK-25857），批次就**在作业照常运行的情况下消失**了。抛异常让 task 失败、
source 重放，是唯一不丢数的选择。

### 4.6 配置校验与约束

| 约束 | 处理 |
|---|---|
| `stateful-2pc` + `sink.group-commit != off` | **启动即报错**：Group Commit 自带事务语义，与 2PC 互斥（构造 `DorisDataSinkOptions` 时校验） |
| `sink.label-prefix` 为空 | **只在真拿它拼 label 的档**启动即报错（`stateful` 且 Group Commit 关、以及 `stateful-2pc`）。legacy 用自己内置的 `cdc_`、GC 打开的 load 根本不带 label，这两处该值是死的，空着不算错 |
| 2PC 需要 Doris ≥ 2.1 | 写进文档，不做运行期探测 |
| 2PC **必须开 checkpointing** | 目标端的依赖：没有 checkpoint 就没有 committer 的提交，行**永远不可见**（ITCase 把它固化成断言） |

### 4.7 Step 0 实测结论（协议细节）

跑在 `apache/doris:fe-2.1.8` / `be-2.1.8` 上，脚本与完整报文留档
`flink-cdc-pipeline-connector-jdbc-kafka-json/src/test/resources/doris/two-phase-commit-experiment.sql`：

| 编号 | 验的是什么 | 结论 |
|---|---|---|
| F1/F3 | `two_phase_commit` 放哪儿 | 必须是**请求头**，且**转向 BE 的第二次请求要重发**——FE 的 307 不是代理，只带一次会让 BE 当普通导入立刻提交 |
| F2 | 预提交成功但不可见 | `Status=Success` + `TxnId`，`count(*) = 0` 直到 commit |
| F4/F5 | commit/abort 端点与路径 | `PUT /api/{db}/_stream_load_2pc`；经 FE 同样 307，`Location` 带 userinfo 与尾随 `?`——现有两步走可复用 |
| F6/F14 | 幂等语义 | 重复 commit → "already visible" → 成功；abort 不存在的 → "not found" → 成功；commit 已 abort 的 → **必须失败** |
| F7/F8 | 按 label 找悬挂事务 | 空 body 预提交同一 label → `Label Already Exists` + 消息里的 txn id（`TxnId` 字段是 -1）；探测**新** label 会真建事务，必须立刻 abort |
| F9 | 权限 | 2PC **不新增权限**：只要今天能 StreamLoad，今天就能 commit/abort（一条 SQL 都不发）。DDL 路径的 `ALTER` 权限是**既有**前提，与 2PC 无关 |
| F11 | 事务/标签存活时间 | 预提交事务闲置上限 `stream_load_default_precommit_timeout_second = 3600`（读自 FE 配置，非观测） |
| F12 | **abort 释放 label** | 同一 label 可再次预提交并 commit（重启复用 label 的地基） |
| F13 | commit 后 label 不再释放 | `ExistingJobStatus=FINISHED`（消息里写 `[VISIBLE]`）→ 不可恢复，只能失败 |

---

## 5. DDL 执行（MetadataApplier + DdlBuilder）

### 5.1 DorisMetadataApplier

运行在 **JobManager 的 schema-evolution coordinator 内**，`instanceof` 分派**全部 10 个事件**到
`DorisDdlBuilder`，生成一条或多条 SQL，经 `DorisHttpClient.executeSql` 依次执行：

```
applySchemaChange(event)
  ├─ buildSqls(event)：instanceof 分派 → DorisDdlBuilder.buildXxxSql(...)
  └─ 对每条 sql：client().executeSql(options.mapDatabase(tableId), sql)
```

- **全接受**（不做 `acceptsSchemaEvolutionType` 收窄）——匹配 coordinator 的透传 derivation；
- **HTTP client 懒创建**：`DorisMetadataApplier` 从 client 序列化到 JobManager，`OkHttpClient` 不可序列化，
  所以字段是 `transient`、首次用时 `synchronized` 建。

### 5.2 DorisDdlBuilder（事件 → Doris SQL）

- **CREATE TABLE**：`CREATE TABLE IF NOT EXISTS db.tbl (cols + COMMENT)`；有主键 → **UNIQUE KEY(pk)
  DISTRIBUTED BY HASH(pk) BUCKETS AUTO**；无主键 → **DUPLICATE KEY(首个物理列) DISTRIBUTED BY
  HASH(该列) BUCKETS AUTO**；metadata 列（非 physical）跳过。
- **类型映射**：所有时间戳 → `DATETIMEV2`（精度 clamp 到 `[0,6]`）；ARRAY/MAP/ROW → `STRING`
  （放行转换器产出的 JSON 文本）。
- **ALTER**：`ADD/DROP/RENAME/MODIFY COLUMN`（多列事件产多条单语句 DDL）；`ALTER TABLE old RENAME TO new`；
  `ALTER TABLE ... COMMENT '...'`；`ALTER TABLE ... MODIFY COLUMN col <type> COMMENT '...'`。
- **RENAME TABLE**：见 §4.4（不是一条 `RENAME` 就完事——名字成环时要换语句形态）。
- **DROP**：`DROP TABLE IF EXISTS`（幂等）；**TRUNCATE**：`TRUNCATE TABLE`。

### 5.3 DDL 的重试策略（与 StreamLoad 不同）

```
executeSql：网络层失败按 max-retries 重试；应用层失败（HTTP code != 0）立即抛、不重试
```

**为什么 DDL 不重试**：DDL 不是幂等的（比如 CREATE TABLE 只生效一次，RENAME 重放会把表再挪一次）。
所以只对网络层错误重试。

### 5.4 表改名（RENAME）的 SQL 生成

`RenameTableEvent` 携带**一条语句的全部 pairs**（有序，见 [03-event-model.md](./03-event-model.md) §3）。
`DorisDdlBuilder.buildRenameTableSql` 先按**映射后的 Doris 库**把 pairs 分组（跨库的一对直接 fail-fast，
理由见 03 文档 §3.5），再对每组循环产出语句，每一轮按优先级选一种：

| 优先级 | 条件 | 产出 | 为什么 |
|---|---|---|---|
| 1 | 队列里存在**两表成环**（`a→b` 且 `b→a`） | `ALTER TABLE db.a REPLACE WITH TABLE b PROPERTIES('swap' = 'true')` | **一条原子语句**，交换两表的名字/schema/数据且两张都保留；没有"两张表都不在自己最终名字上"的窗口 |
| 2 | 存在一对的**目标名没被其它待办改名占着** | `ALTER TABLE db.a RENAME b` | 直接改，Doris 只接受不带库名的第二个名字 |
| 3 | 其余（≥3 表成环，或 2 表环走了更长的链） | 把其中一张移到临时名 `__cdc_tmp_<表名前32字节>_<hash>`，把它加回队尾待办 | 一条 `ALTER TABLE` 只能改一张表，`REPLACE` 只能换两张，环必须靠临时名解开 |

每轮要么完成一次改名、要么腾空一个名字，队列严格变小（轮数上限 `2n+1` 只是防"推理写错了转死循环"）。

**为什么临时名是确定性的**（表名 + hash 而非 uuid）：同一条语句重跑产生同一批 SQL；否则失败重启后
会在库里堆一串不同的 `__cdc_tmp_*`。名字长度也压过（表名截 32 字节 + 计数器），仍在 Doris 的 64 字节
表名限制内。

**如实记录的两个代价**：
- 优先级 3 的临时名路径**不原子**：这条语句与"把表移到最终名"的那条之间失败，表就停在临时名上，需要人工
  收尾。这是 ≥3 表环唯一的表达方式（生产者把对调写成临时名链时，连接器在 source 侧已经折成净改名，见
  03 文档 §3.3，所以正常对调走的是优先级 1 而非这里）。
- DDL **不重试**（§5.3）意味着"部分执行"确实可能发生。把对调压成一条 `REPLACE` 正是为了消灭这个窗口。

### 5.5 Step 0：Doris 交换语义实测结论

跑在 `apache/doris:fe-2.1.8` / `be-2.1.8` 上，**同时经 FE MySQL 9030 与 HTTP
`/api/query/default_cluster/{db}`**（后者是 `DorisMetadataApplier` 的真实通道）。脚本与完整结论留档
`flink-cdc-pipeline-connector-jdbc-kafka-json/src/test/resources/doris/rename-swap-experiment.sql`：

| 编号 | 验的是什么 | 结论 |
|---|---|---|
| E1/E2 | 结构、表模型（UNIQUE ↔ DUPLICATE）、主键完全不同的两表互换 | **可以**，不被拒 |
| E3 | **不带 `PROPERTIES` 的** `REPLACE WITH TABLE` | 默认就是 `swap='true'`，两张表都保留 —— 但生成 SQL **永远显式带该属性**，不依赖默认值 |
| E4 | 交换后 `SHOW CREATE TABLE` + `SELECT` | schema **与数据**都跟着名字走 |
| E5 | 能否经 FE HTTP 通道执行 | 可以 |
| E6 | 第二个表名带库名 | 必须**不带**（在第一个表的库里解析）→ 跨库改名不可表达 |
| EA | `ALTER TABLE g1 RENAME g2`（g2 已存在） | 报 `Table name[g2] is already used` → 目标名必须先腾空，这正是优先级 2/3 存在的原因 |
| EB | 自我改名（old == new） | 报 `Same table name` → 映射后同名的一对被**跳过并告警**（不生成 SQL），不会打到 Doris |
| EC | 一条 `ALTER TABLE` 改多张表 | 语法错误，一次只改一张 |

---

## 6. 代码清单

Doris 包按「域」分了子目录，路径都相对 `sink/engine/doris/`（pipeline 模块 `.../connectors/kafkajson/`）：

```
sink/engine/doris/
├── DorisDataSinkDialect / DorisDataSinkOptions / DorisRowConverter / DorisWriteMetrics   共享件
├── writer/   三档 sink 与 writer（legacy ｜ stateful ｜ stateful-2pc）
├── state/    writer 状态 + 序列化器
├── commit/   两阶段的 committable / committer
├── ddl/      DDL 生成与执行
└── http/     HTTP 客户端
```

| 类 | 位置 | 职责 |
|---|---|---|
| `DorisSink` / `DorisSinkWriter` | `writer/` | legacy 档：非 stateful、非 2PC（§3） |
| `StatefulDorisSink` / `StatefulDorisSinkWriter` | `writer/` | `stateful` 档：`StatefulSink` + 可 checkpoint 的 sequenceCounter；也是 2PC writer 的基类 |
| `TwoPhaseDorisSink` / `TwoPhaseDorisSinkWriter` | `writer/` | `stateful-2pc` 档：`StatefulSink` + `TwoPhaseCommittingSink`；epoch/rung label、预提交、label 冲突清理 |
| `DorisWriterState` / `DorisWriterStateSerializer` | `state/` | writer 状态（labelPrefix + sequenceCounter）与序列化器 |
| `DorisCommittable` / `DorisCommittableSerializer` | `commit/` | 交给 committer 的一笔事务（db/table/label/txnId） |
| `DorisCommitter` | `commit/` | checkpoint 完成后逐条 `commit`；失败**抛异常**（FLINK-25857） |
| `DorisHttpClient` | `http/` | OkHttp：`streamLoad`（307 两步走 + label 幂等重试）+ `precommitStreamLoad` / `commitTransaction` / `abortTransaction` + `executeSql` |
| `DorisDdlBuilder` | `ddl/` | 10 事件 → Doris SQL |
| `DorisMetadataApplier` | `ddl/` | coordinator 内执行 DDL |
| `DorisDataSinkOptions` | 包根 | Doris 配置（FENODES/USERNAME/PASSWORD/buffer/flush/前缀后缀/max-retries/**`sink.writer`**/**`sink.label-prefix`**） |
| `DorisDataSinkDialect` | 包根 | extends `KafkaJsonDataSinkDialect`，组装 sink/applier/converter；按档分派 sink |
| `DorisRowConverter` / `DorisWriteMetrics` | 包根 | 行转换 / 写吞吐指标 |
| `KafkaJsonRowConverter` | `sink/converter/` | 行转换抽象基类 |
| `DorisSinkExample` | `test/example/`（test 源集） | 完整组装，可跑 MiniCluster；示例不随连接器 jar 发布 |

---

## 7. 风险与边界

| 风险 | 应对 |
|---|---|
| source 与 sink 并行度不一致 | sink 链每个算子都取**输入流的并行度**（且**逐个显式设置**——`.map()` 默认取 env 并行度），所以 source 的并行度就是整条链（也就是整个作业）的并行度；不一致时 Flink 会在 source 与 schema 算子之间插 rebalance，按主键有序的前提被破坏（见 [ARCHITECTURE.md §3.4](../ARCHITECTURE.md)） |
| **同一行可能被写两次**（source 侧改名后/运行期新表不等回填就发，见 [01-exactly-once.md §5.9](./01-exactly-once.md)） | 幂等吸收：有主键的源表在 Doris 侧由 CREATE 建成 **UNIQUE KEY(pk)**，每行都按 upsert 写（§3.4），重复的同一变更覆盖成同一个值（无主键的表本就过不了 source 侧的分片切分，见 `KafkaJsonChunkUtils.getSplitColumn`）。这是有意选的一侧——反过来"等回填"在改名场景下等不到，就是丢数据 |
| FE 执行 DDL 需 `is_execute_sql_in_http=true`（默认 false） | 主走 HTTP；无法开启时后续切 MySQL 协议 JDBC（可选项，未实现） |
| StreamLoad 非 2PC = 至少一次 | 每 checkpoint/flush 一次 PUT + label 幂等；要精确一次语义的可见性边界就用 `stateful-2pc`（§4） |
| **2PC 必须开 checkpointing** | 没有 checkpoint 就没有 committer 的提交 → 预提交的行**永远不可见**（事务最终被 Doris 超时回收）。`DorisTwoPhaseCommitITCase` 把这一点固化成断言：宁可写死，也不让"配了 2PC 却忘了开 checkpoint"变成静默不落库 |
| **预提交事务闲置 3600s 会被回收**（F11） | checkpoint 间隔、背压、或停机时间长于此值时，`commit` 会撞上 "not found"/"already aborted" 而失败 → 作业重启重放。要更长的窗口就调 Doris 的 `stream_load_default_precommit_timeout_second`；连接器不做"超时前主动 commit"（那会破坏 checkpoint 的原子性） |
| **孤儿事务（无人认领的预提交）** | 恢复时**不主动** abort 任何东西（理由见 §4.2）。能收的就两种：写路径撞上同 label 时按 Doris 报出的事务号 abort（F12），或 Doris 自己按 3600s 超时回收 |
| 2PC 撞上 `FINISHED` 的 label | **响亮失败**并点名 `sink.label-prefix`，不静默跳过：这意味着 label 空间被别的东西用过，重试不会变好 |
| Doris 2.x 拒绝 `enable_batch_delete_by_default` | 改用 `hidden_columns=__DORIS_DELETE_SIGN__` header |
| OkHttp 不可序列化（MetadataApplier 跨 client→JM 序列化） | `transient` + 懒初始化 |
| `is_execute_sql_in_http` 不可用时 DDL 全挂 | 阻塞协议会把作业卡死在 APPLYING → 需按上表切换 JDBC |

---

## 8. 验证

```bash
# 单测（Doris 用 JDK com.sun.net.httpserver.HttpServer 模拟，无 Docker）
mvn -q -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test -Dtest='DorisHttpClientTest,DorisSinkWriterTest,DorisDdlBuilderTest,DorisMetadataApplierTest,DorisRowConverterTest' \
  -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true
```

要点：`MockDorisServer`（`test/.../http/`）模拟 FE/BE，断言 StreamLoad 请求路径/header（label、format=json、
hidden_columns、Authorization）/body、`Status!=Success` 时按 label 幂等重试、`executeSql` 请求路径与 body；
`DorisSinkWriterTest` 断言 DELETE 带 `__DORIS_DELETE_SIGN__`、schemaMaps 演进（标准 + 自定义事件）。

§4 新增的验证（同一套 `MockDorisServer`，**无 mockito**）：

| 测试 | 断言什么 |
|---|---|
| `DorisHttpClientTwoPhaseCommitTest` | `two_phase_commit: true` **两腿都带**、缺 `TxnId` 报错、label 冲突报出旧事务号、`already visible`/`not found` 当成功、`already aborted` 抛错、`_stream_load_2pc` 的 307 跟随 |
| `TwoPhaseDorisSinkWriterTest` | label 形状与 epoch 推进（`snapshotState` 后进新 epoch 且 rung 归零）、`prepareCommit` 交出后清空、冲突 → abort → 同 label 重载、`FINISHED` 抛错并点名配置项、**`close()` 不 flush** |
| `StatefulDorisSinkWriterTest` | 状态 round-trip、恢复取 `max(状态+1, 墙钟)`、1PC 档仍是普通 PUT |
| `DorisCommitterTest` | 逐条 commit、空集合不发请求、失败**抛异常而非 signal**（断言 `signals` 为空） |
| `DorisTwoPhaseCommitITCase`（MiniCluster + 真实 sink 链） | ① `stateful` 档一次都不碰 `_stream_load_2pc`；② `stateful-2pc` **不开 checkpoint 时行永远不可见**；③ 开 checkpoint 后行可见、无重复 commit、label 形状正确；④ **故障重启**：死掉那次尝试的 6 个事务被逐个 abort，同 label 重载后可见 |

> 运行方式：本仓库 `test` 阶段只跑 `**/*Test.java`，`*ITCase` 在 `integration-test` 阶段跑。只想验证 2PC 时
> `mvn -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json test -Dtest=DorisTwoPhaseCommitITCase` 即可
> （`-Dtest` 覆盖 includes）。同模块的 MySQL/Doris 端到端 ITCase 需要 Docker，不在此列。

真实 Doris 容器端到端验证列后续阶段（协议细节已按 §4.7 实测固定）。
