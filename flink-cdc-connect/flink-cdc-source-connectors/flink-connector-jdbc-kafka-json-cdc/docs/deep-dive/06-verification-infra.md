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

# 验证基建：账本对账与可观测性

> 本文讲怎么证明连接器**真的对**：用真实数据 + 独立于被测件的 ground-truth 账本 + 对账，
> 把"无重无丢、最终收敛"定义成可观测的属性，而不是断言"固定场景的精确事件序列"。
> 关联：[01-exactly-once.md](./01-exactly-once.md)（要验证的机制）、[ROADMAP.md](../ROADMAP.md)（历史修复点）。
> 代码位置：pipeline 模块 `src/test/.../reconcile/` + `src/test/.../source/` 的 ITCase。

---

## 1. 设计取向：属性级、黑盒、收敛型断言

现有 ITCase（如 `MySqlCanalChainITCase`）用 `containsExactly(...)` 断言**精确事件序列**——只证明一条固定
路径对，遇到合法的 wire 形状差异（比如 canal 半行 `old` vs TiCDC 全行 `old`）就脆断。用户真正关心的是
**镜像表最终等于源表、无丢无重、值类型保真**。

校验方案因此定为三件事：

1. **被测件保持黑盒**——只用它对外输出的 `Event` 流判断，不注入探针进连接器内部；
2. **ground-truth 账本与连接器完全独立**——`WorkloadDriver` 以普通 JDBC 客户端身份写源库，记录"我执行了什么"，
   构成"应该发生什么"的权威答案，**不经过连接器**；
3. **收敛型断言**——合法 wire 形状有差异也没关系，只要下游收敛到源库，正确性判定就成立。

---

## 2. 五条可观测的正确性属性

| # | 属性 | 可观测信号 | 判定方法 |
|---|---|---|---|
| P1 | 状态收敛 | 镜像表 vs 源表逐主键行指纹 diff | diff 集合为空 |
| P2 | 无重无丢 | 账本 vs 事件流逐 key 对应 | 每个 DML 恰好映射一次；无孤儿事件；无重复 INSERT |
| P3 | 值/类型保真 | 行指纹（规范化序列化，含类型 / NULL） | 逐列一致 |
| P4 | 边界/切换正确 | 快照→增量切换窗口内无重复 + 最终收敛 | 边界场景 + 收敛断言 |
| P5 | 恢复/断点续传正确 | 重启后 lag 归零、重复计数不增、仍收敛 | 韧性场景 |

---

## 3. 已落地的对账基建（`src/test/.../reconcile/`）

> 对应规划文档里的"Mode A（事件级对账）"——直接验证连接器输出的事件流本身，命中 P2/P3/P4，
> **不依赖任何 sink**。Mode B（镜像表状态级对账）列为后续增强。

### 3.1 三个核心组件

```
WorkloadDriver（JDBC 写源库 + 内存期望表模型 + 逐操作记 Ledger）
        │
        ▼
源库（MySQL / TiDB）──binlog──► canal-server / TiCDC ──► Kafka
        │                                          │
        │                                          ▼
        │                          连接器（被测，纯黑盒）
        │                                          │  Event 流
        │                                          ▼
        │                          EventCollector（测试内收集）
        │                                          │
        ▼                                          ▼
      Ledger（账本：应该发生什么）          EventArchive（实际收到什么）
        └──────────────────► LedgerVerifier.verify（逐 key 对账 + 重放收敛）──► Violation 列表
```

| 组件 | 职责 | 关键点 |
|---|---|---|
| `WorkloadDriver` | `initSchema()` + `insertInitialRows(n)`（快照前基线，phase=INITIAL）+ `runRandomDml(ops, insertPct, updatePct)` | 单线程 JDBC；维护 `expectedModel`（id→row）；**每次成功 DML 记一条 Ledger** |
| `Ledger` / `LedgerEntry` | 账本；每条含 `pk / op / row / phase` | phase 分 `INITIAL`（快照前基线，预期由 JDBC 快照发）与 `POST_SNAPSHOT`（预期为独立流事件） |
| `EventArchive` | 连接器实际产出的 `DataChangeEvent` 按主键存序 | INSERT/REPLACE→INSERT、UPDATE→UPDATE、DELETE→DELETE；值从 `after`/`before` 的 `RecordData` 提取；CreateTableEvent 忽略 |
| `LedgerVerifier` | `verify(ledger, archive, expectedModel, sourceFinalState)` → `List<Violation>` | 逐 key 比"预期事件序列 vs 实际事件序列"（多=重复、缺=丢失、值不同=差异）；再重放事件到内存模型验证末态 |

### 3.2 对账判定的四类违规

`LedgerVerifier.Violation.Type`：

| 违规类型 | 含义 | 命中属性 |
|---|---|---|
| `EVENT_SEQUENCE_MISMATCH` | 事件序列 vs 账本序列逐 index 对不上（多余=重复，缺失=丢失，值不同） | P2 / P3 |
| `ILLEGAL_SEQUENCE` | 结构性非法：已存在时再 INSERT（重复）、不存在时 UPDATE/DELETE | P2 |
| `STATE_MISMATCH` | 事件重放重建的末态 ≠ driver 期望表末态 | P3 |
| `SOURCE_MODEL_MISMATCH` | **driver 期望模型 ≠ 真实源表末态**（工作负载自身没按预期跑，driver 的 bug，不是连接器） | —（看门狗） |

### 3.3 预期事件序列怎么构造（`expectedEventsFor`）

对每个 key：
- `INITIAL` 账本条目 → 预期一个 `INSERT`（由 JDBC 快照发出，不要求是独立流事件）；
- `POST_SNAPSHOT` 账本条目 → 按 `op` 映射为恰一个 `INSERT/UPDATE/DELETE`，**按账本（binlog）顺序**。

对账**按 key 分组比较**，天然绕开 Kafka 跨分区乱序问题。Driver 的单线程执行序 == binlog 序 ==
canal 写 Kafka 序，所以账本序就是预期事件序。

> **为什么不用 es 直接对账**：driver 拿不到 canal 分配的 binlog `es`。账本用执行序，按 key 重建序列即可；
> `es` 只用于边界窗口的深层诊断（[01-exactly-once.md](./01-exactly-once.md) 的边界场景）。

---

## 4. 端到端验证：`KafkaJsonExactlyOnceITCase`

真实链路：MySQL 8 + `CanalServerContainer` + Kafka + 连接器，流程：

1. **快照前基线**：driver 种 50 行（phase=INITIAL，记入 Ledger）；canal 启动；连接器 JDBC 快照；
2. **快照阶段断言**：恰好收到 50 个 `DataChangeEvent`（CreateTableEvent 单独收集）；
3. **留出切换窗口**（`Thread.sleep(5000)`，让 HIGH 捕获 + stream split 分配完成，之后的写入才是独立流事件）；
4. **快照后随机 DML**：driver 执行 30 个随机操作（默认 40/40/20 insert/update/delete），逐个记 Ledger；
5. **排空**：drain 到流安静（`drainUntilQuiet`，8s 无事件）或超时；
6. **对账**：`LedgerVerifier.verify(ledger, archive, driver.expectedModel(), driver.querySourceFinalState())`
   → 断言 `violations` 为空。

工具细节：
- `EventCollector` 独立线程收集 job 的 `CloseableIterator<Event>`，`pollOrFail` 把 job 失败变成测试失败（而非静默超时）；
- `querySourceFinalState()` 跑完后再查一次源表真实末态，交给 `SOURCE_MODEL_MISMATCH` 兜底。

---

## 5. 断点续传验证：`KafkaJsonOffsetCommitITCase`

验证流式 consumer 的 **checkpoint-completion 才提交 offset** 语义（[01-exactly-once.md](./01-exactly-once.md) §6）：

- 消费第一批 → **第一个 checkpoint 完成后**，group offset 恰好提交到这批的末尾；
- checkpoint 前消费但未提交的消息，**不得推进** group offset；
- 下一个 checkpoint 后再推进一条。

用 Kafka `AdminClient` 直接查 consumer group 的 committed offset 断言。

---

## 6. 尚未落地（规划中的可观测性增强）

> 规划文档（VERIFICATION_PLAN）的设计目标，实现按阶段推进；下面这些是**未来工作**，不是现状。

### 6.1 Mode B：镜像表状态级对账（命中 P1/P3 的最终形态）

自写「镜像 Sink」测试 util（模式参考 `KafkaJsonRenameStateOperator`）：

- `CreateTableEvent` → 建镜像表（schema 用连接器类型映射，保证确定性）；
- `AddColumn / DropColumn / AlterColumnType` → ALTER 镜像表；`RenameTableEvent` → 迁移镜像表；
- `DataChangeEvent` → 按主键 UPSERT / DELETE（幂等，吸收"已发表去重"事件）；
- `Reconciler`：源表 vs 镜像表逐 key 行指纹 + count + 数值列 checksum → 差异报告（可观测证据）。

### 6.2 审计元数据（可离线重放 + 边界诊断素材）

`KafkaJsonPipelineRecordEmitter.processElement` 里 `SourceRecord` 已带 `sourceOffset()`
（es/partition/offset）与 `source.ts_ms`，但转 `Event` 时丢了。规划：包一层
`AuditedEvent { Event; es; ts; partition; offset; TableId }` 旁路写到审计 topic——不改 `Event` 类、
不改 released 序列化栈。这是边界窗口重复/丢失诊断 + 离线重放的数据底座。

### 6.3 指标与在线不变量

- 连接器侧：复用 base `SourceReaderMetrics` + 每表每 op 事件计数 / watermark / split 进度 / Kafka lag；
- 验证侧：违规计数器 + 对账 diff 计数，CI 用 MiniCluster metric query service 直接读断言；
- 在线不变量算子（ProcessFunction）：per-key op 序列合法、单分区内 es 单调不减（需审计元数据）、
  末态逐步比对——违规即计数+1 并写违规日志，不悄悄错。

---

## 7. 场景矩阵（规划）

确定性场景定位具体 bug（可复现）：快照期写入（P0-4 重复根因）、多分区 + 大表多 chunk + 边界时刻写、
流中 DDL storm（ADD / DROP / MODIFY / RENAME COLUMN / RENAME TABLE）、no-PK 表、checkpoint→kill→恢复、
多库多表、全类型值往返、并行度 2+。随机化场景广撒网（多 seed 随机 DML ± DDL、长跑 + 中途恢复）。

覆盖矩阵：`MySQL+canal / TiDB+TiCDC` × `initial / snapshot / stream-only` × 并行度 1/2 ×
`单表 / 大表多 chunk / 多表` × 三种写相位。

---

## 8. 验证方式

```bash
# 端到端 exactly-once 对账（Docker：真实 MySQL + canal-server + Kafka）
sg docker -c "mvn -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test-compile surefire:test@integration-tests \
  -Dtest='KafkaJsonExactlyOnceITCase,KafkaJsonOffsetCommitITCase' \
  -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true"
```

> 每次跑完把 Violation 列表打日志留痕。任一真实链路暴露差异 → 如实报告，回计划层决策，不静默掩盖。
