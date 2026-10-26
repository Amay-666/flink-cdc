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

# Roadmap / 优先级规划

> 分支：`feature/3.2.1-jdbc-kafka-json`
> 状态：2026-08-13 排定；Phase 11 集成测试已全绿（f48f1e8d）；2026-10-23 增补 P5（性能/规模化）。
> 总原则（用户拍板）：**先保证数据 exactly-once，再保证数据转换正确性；Debezium 相关不急。**

优先级：P0 = exactly-once 正确性（阻塞性）｜P1 = 数据转换正确性｜P2 = TiDB 链路补强｜P3 = 健壮性 / DDL 扩展｜P4 = Debezium（低优先）｜P5 = 性能 / 规模化（吞吐与状态成本）

**状态图例**：⬜ 待办｜✅ 已落地｜⏸️ 维持结论（不需修）

---

## P0 — Exactly-Once 正确性（先做，阻塞后续所有链路可信度）

### P0-1 流式边界语义审计与修复 ✅

`KafkaJsonStreamFetchTask#processRecords` 的 END watermark 归属 + 有界回填越界问题。

**结论与修复（2026-08-13）：**
- **END watermark 归属：不是 bug。** base 的 `AbstractScanFetchTask.execute` 只在 `!streamBackfillRequired`
  （low==high）时自己派发 END；进入 backfill 就调用 `executeBackfillTask`，由其负责派发 END。
  `KafkaJsonStreamFetchTask` 在有界回填里 `dispatchEndWatermark` 与 MySQL 连接器模式一致，属正确。
- **真正的 bug：有界回填整批超发。** 旧实现消费**一整批**后才看 `currentOffset >= endingOffset`，
  es ≥ HIGH 的消息（快照期间新到变更）被回填发出；而流式 split 从 `offsetsForTimes(HIGH)` 开始读，
  `shouldEmit` 对这批 es≥HIGH 记录再放行一次 → **重复**。
- **修复**：有界读把 ending offset 当**排他**上界（es ≥ ending 一律不 emit，归流式阶段）；终止条件是
  **每个**已分配 partition 都出现 es ≥ ending（或排空到 log end）才 `dispatchEndWatermark`，不能见首个越界就停。
  - 单 split：回填发 `[LOW, HIGH)`，流式发 `[HIGH, …)`，恰一次。
  - 空 partition（end==0）直接视为 done，避免挂死。
- **测试**：`KafkaJsonStreamFetchTaskTest` 新增「两个 partition，一个先越界、另一个还有 es<ending 未读」用例；
  `testBoundedReadDispatchesEndWatermark` 改为断言 es==ending 的记录**不** emit；`KafkaJsonScanFetchTaskTest`
  全管道用例改为 6 条。**4 个 Phase-11 ITCase 全绿。**

### P0-2 split 完成后仍读 Kafka 数据 + 重复 CreateTableEvent（多 subtask） ✅/⏸️

- **"Finished reading split 仍然会读取 kafka 数据"：不是 bug。** 快照 split 全部读完、END watermark 派发后进入
  **流式阶段**，这正是设计的正常行为。
- **重复 CreateTableEvent（来自不同 subtask）：真实存在但幂等。** `KafkaJsonPipelineRecordEmitter` 的
  `alreadySendCreateTableTables` 是**按 emitter 实例（subtask）隔离**的普通 HashSet。标准 pipeline 框架下
  由下游 `SchemaManager.isOriginalSchemaChangeEventRedundant` 判为 redundant 忽略，幂等；**裸 source 用法**
  （用户把 pipeline 模块当 Flink source 直接 `env.fromSource`，下游无 SchemaManager）下重复事件会直达用户 sink。
- **处理**：裸 source 用法建议**并行度 = 1**；并行度 > 1 仅当下游对 CreateTableEvent 幂等时使用。
  跨 subtask 去重需要 distributed state，emitter 拿不到（不碰 released 模块），与官方一致不予实现。

### P0-3 表 schema 注册并行化问题 ✅

`KafkaJsonDialect` 持有的 `KafkaJsonSchema` 被两类线程并发访问（enumerator 的 chunk splitter 与
stream-split reader 的 schema 发现），HashMap 有数据竞争风险。

**修复（source 模块内，不改 released）：**
- `KafkaJsonDialect`：`schema`/`filters` 改 `volatile` + synchronized 双重检查懒初始化；
- `KafkaJsonSchema`：`getTableSchema`/`registerTable`/`removeTable` 加 `synchronized`。
- per-subtask 的 schema/recordFactory 无跨 subtask 共享，竞态仅限 dialect 共享实例的解析入口。
- **验证**：source 模块 144 单测全绿。

### P0-4 快照↔流式衔接重复：min 水位把「读库期间提交的变更」漏到流式侧 ✅

用户实测：快照阶段写入一条 UPDATE，JDBC 快照读到它的效果，同时 canal 日志在快照完成后又被流式捕获下发 → **重复**。

**根因**：`queryCurrentOffset` 返回各 partition 最新消息 es 的**最小值** `(min, -1, -1)` 作 HIGH 水位。
读库期间提交的变更 E 若 `min < E ≤ 真实边界`：有界回填以 `es ≥ HIGH` 排他 → 剔除 E；流式 `isAfter((min,-1,-1))`
→ 放行 E；而快照已读到 E 的效果 → 下游收到「快照行 + 同一变更」= 重复。

**修复（source 模块内）：**
1. `queryCurrentOffset` 改为各 partition 最新消息 es 的**最大值** + sentinel 分区/偏移 `(max, Integer.MAX_VALUE, Long.MAX_VALUE)`。
   真实记录分区恒 < MAX，故 es==max 的边界记录排在水位**之前**（`isAfter` 为 false）→ 归属回填。
2. `KafkaJsonStreamFetchTask` 有界读：上界改为 `lastOffset.isAfter(endingOffset)`（边界记录含端点、由回填恰好发出一次）；
   新增下界 `es < starting` 一律丢弃（canal 滞后消息不覆盖快照行）。
   - 语义变为：单 split 回填发 `[LOW, HIGH]`（含端点），流式发 `(HIGH, …)`。

**验证**：`KafkaJsonStreamFetchTaskTest` 新增 2 例（边界消息被回填保留；es<starting 被下界丢弃）；
source 模块 155 单测全绿；4 个 Phase-11 ITCase 不受影响。

**残余（与 base 一致，未修）**：多 split 之间某行变更若其 chunk 已读完，依赖下一 split 的 JDBC 读补**终值**。

---

## P1 — 数据转换正确性

### P1-1 `KafkaJsonValueConverter` 常见类型数值转换测试 ✅

`KafkaJsonValueConverterTest` 由 12 例扩到 21 例（int/bigint 极值、FLOAT/DOUBLE 科学计数法、DECIMAL(38)、
varchar 边界、TIME(6) 小数秒、日期上下界、TIMESTAMP 闰日微秒、非法值抛 `FlinkRuntimeException` 契约）。

### P1-2 `KafkaJsonTypeUtils` Datetime / Timestamp 转换异常 + 列长超限 ✅

**根因** = JDBC 元数据对 temporal 报的是**显示宽度**（DATETIME(6)→26、TIME(6)→15 等），超过 Flink
`TIMESTAMP(p)`/`TIME(p)` 的 p∈[0,9] 上限 → 快照建表 `ValidationException`。修复（P1-3 的 `KafkaJsonColumnMeta`）：
**从显示宽度反推 FSP**——DATETIME/TIMESTAMP `width>19 ? width-20 : 0`，TIME `width>8 ? width-9 : 0`。
关键：canal 消息路径（`KafkaJsonTableUtils.buildColumn` 解析 DDL 文本）的 length 已是真 FSP，
与 JDBC 路径的显示宽度语义不同；两者以 `length≤6`（FSP 上限）分界。修复后快照与流发出的 DataType 精度一致。

### P1-3 两份 `KafkaJsonTypeUtils` 整合简化 ✅

pipeline 模块与 source 模块各有一份 `KafkaJsonTypeUtils`（返回 cdc / flink table 两种 DataType）。
修复：新 `source/utils/KafkaJsonColumnMeta.java` 作为唯一共享映射（归一化 + clamping + cdc DataType 映射）；
source `KafkaJsonTypeUtils.fromDbzColumn` 纯委托 `DataTypeUtils.toFlinkDataType(...)`；pipeline 副本删除。
`KafkaJsonColumnMetaTest` 10 例、`KafkaJsonEventDeserializerTest` 11 例全绿。

---

## P2 — TiDB 链路补强

### P2-1 TiDB canal 消息额外处理 ✅

1. **剔除 `TIDB_WATERMARK` 类型消息**（`isDdl=false` 且 `type=TIDB_WATERMARK` 时不是 DML）：
   `KafkaJsonRecordConverter.convert` 显式 `case "TIDB_WATERMARK"` 过滤并注释。
2. **DELETE 事件兼容**：v5.4.0 起 TiCDC 的 DELETE `old` 为 `null`、被删数据在 `data` 里。
   `convertRows` 的 DELETE 分支始终以 `data`（before 行）作前像，`old` 为 null/absent 无影响。

**端到端实证**：`TiCDCServer.createChangefeed` 的 sink-uri 加 `enable-tidb-extension=true`，
`TiDBCdcChainITCase` 全绿——watermark 被正确剔除、`_tidb` 扩展字段被 parser 容错。

### P2-2 TiDB TSO → 时间戳（event_time / 水印基础） ✅

前置调研：真实 TiCDC canal-json 里 `es`/`ts` 都是可靠 Unix 毫秒，**P2-2 不升级 P1**。
采纳用户提议——TiDB 链路的快照低/高水印改为**直接查数据库当前 TSO**（`KafkaJsonTidbOffsetUtils`：
`BEGIN` 内 `SELECT TIDB_CURRENT_TSO()`，`tso >> 18` 转物理毫秒，打 sentinel；失败返回 `null` 回退 Kafka 采样）。
`KafkaJsonDialect.displayCurrentOffset`：`scan.database.type=tidb` **且** `scan.message.event-time=es` 时走 TSO 路径。

**为什么比 Kafka 采样更对**：Kafka 采样的 H = 最新已发布消息的 es，被发布延迟拖后于库「now」→
`H_kafka < es ≤ 快照结束` 的事件会被重复下发；topic 空时回 `INITIAL_OFFSET` 会触发 earliest 重读=重复 / latest 越界=丢。
TSO 的 H ≥ 快照所有已捕获 commit_ts、且 topic 空也为非零 → 重复窗口塌缩。

---

## P3 — 健壮性 / DDL 扩展

### P3-1 时钟/时区一致性检测工具类 ⬜

一个工具类，检查并报告：数据库服务器时钟、Kafka 集群时钟、运行环境时钟、各自时区是否一致；
启动时打警告或 fail-fast（可配置）。基于 [01-exactly-once.md](./deep-dive/01-exactly-once.md) 的时钟一致前提。

### P3-2 ALTER schema change 事件扩展 ✅（已基本完成）

支持 `TRUNCATE TABLE`、`RENAME COLUMN`、`MODIFY COLUMN COMMENT` 等 DDL。现状：
- RENAME COLUMN：`KafkaJsonDruidDdlParser` 认 `SQLAlterTableRenameColumn`，pipeline 侧还有同位置同类型启发式兜底；
- 连接器自定义事件（RenameTable / DropTable / TruncateTable / AlterTableComment / AlterColumnComment）
  已落地并被自写 coordinator / DorisMetadataApplier 完整消费（见 [deep-dive/03](./deep-dive/03-event-model.md) 与 [deep-dive/05](./deep-dive/05-doris-sink.md)）。

### P3-3 表改名（RENAME TABLE）以 SQL 为权威 + 多对原子事件 ✅（2026-09-15）

**问题**：canal/TiCDC 的 `table` 字段是**改名后**的名字，Debezium 的 `source.table` 是整条语句所有名字的
逗号串、甚至为 `null`。原实现假定 `table` 是**老名**，于是老 schema 取不到、新表注册为空，rename 之后该表
的数据消息直接抛异常；Doris 侧还生成了 `ALTER TABLE db.x RENAME x` 自改名。

**修复**：
- 前后表名**一律从 DDL SQL 解析**（`KafkaJsonDruidDdlParser` 遍历 `MySqlRenameTableStatement` 的全部
  items；Debezium 解析器同源，`findRenamedTable` 的 diff 降级为兜底）；旧表 schema 未知 → **fail-fast**。
- 一条语句的多对是**一个** `RenameTableEvent`（`getPairs()` 有序 + 原始 SQL；`tableId()`/`getSchema()` =
  第一对，单对路径行为不变）；history record 的自定义字段由单值 `canalNewTableId` 换成 `renamePairs` 数组。
- source 侧把**经临时名中转**的对折叠成"净改名"（`RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b` → `a→b` +
  `b→a`），否则下游会去改一张它从未见过的 `a_tmp` 并留下野表。
- Doris：两表成环 → 一条原子 `ALTER TABLE db.a REPLACE WITH TABLE b PROPERTIES('swap'='true')`；否则逐条
  `RENAME`；≥3 表环 → 确定性临时名解开。跨（映射后）库的一对直接 fail-fast。
- 四类生产者的真实抓包落在 `src/test/resources/kafkajson/captured/`（逐字节原样、永不修改），
  `KafkaJsonCapturedMessageTest` + pipeline 侧 `KafkaJsonRenameFixtureTest` 端到端回放。

**注意**：事件序列化格式随之变化，**旧 checkpoint 不兼容**（同分支未发布）。**已知限制**：TiCDC 的
`debezium` 协议 8.5.1 **完全不发 DDL**，该协议下流中 DDL 不可见；生产者把多对语句拆开发时（TiCDC、Debezium）
下游失去原子性（逐条执行仍正确）。

详见 [deep-dive/02](./deep-dive/02-message-parsing.md) §8、[deep-dive/03](./deep-dive/03-event-model.md) §3、
[deep-dive/05](./deep-dive/05-doris-sink.md) §4.4–4.5。

### P3-4 改名后的表不再丢数，且改名过的作业故障重启不丢数不报错 ✅（2026-09-16）

**问题**（P3-3 把 rename 事件做对之后实测暴露的下一环）：改名之后的 data change event 被
`shouldEmit` 判成"不该发"而丢掉。根因是判据错位——"这条消息属于哪个快照分片"那份信息
（`finishedSplitsInfo` / `maxSplitHighWatermarkMap`）记的是**改名之前**的名字，改名后消息里的新名字一条都
匹配不上，而 release 的兜底是 `return false`（把"无法判断"当成"不要发"）→ **整表漏发**。

**修复**（三处，缺一不可）：
- `KafkaJsonIncrementalSourceStreamFetcher.shouldEmit`：判据由"水位"扩为"**schema store 里有没有这张表**"
  ——表在 store 里但不在任何分片信息里（改名后的表、运行期新建的表）→ 发。判据选 schema store 而非
  split 的 `tableSchemas`，因为前者正是**重启后能从 checkpoint 重建**的那份状态。
- 同一方法：**表已不在 schema store 时不做水位判断**。水位判断要取该表主键列（split column），表被
  DROP/改名后取不到 → `getSplitColumn(null)` NPE → 读失败；更糟的是重启后重建出**同一个状态** → 确定性
  失败循环。这类"已在队列里、DDL 刚对它生效"的在途记录直接放行（队列 FIFO，它们必然排在那条 DDL 之前
  到达 sink，sink 侧该表还在）。
- `KafkaJsonSchemaChangeHandler.handle`：schema-change 记录**无条件入队**（不再由 `include.schema.changes`
  在源头拦掉）。base emitter 要靠它把被改的表写进**进 checkpoint 的 split state**，开关只决定"是否发给
  下游"。

**代价（有意选的一侧）**：运行期新建 / 改名后的表从事件起就发，不等回填 → 同一行可能发两次。漏发是丢数据，
多发只要求 sink 幂等（Doris 按主键 upsert 吸收，见 [deep-dive/05](./deep-dive/05-doris-sink.md) §7）。

**测试**：`KafkaJsonIncrementalSourceStreamFetcherTest`（改名当次发 / 重启后发 / 分片内未越水位仍丢 /
表已从 schema 消失不抛异常）四用例。

详见 [deep-dive/01](./deep-dive/01-exactly-once.md) §5.9–5.10、[deep-dive/03](./deep-dive/03-event-model.md) §3.6。

### P3-5 Doris sink 三档写入：状态 + 可选两阶段提交 ✅（2026-09-20）

**起因**：审计 sink 在重启/failover 下的正确性，提出「改 `StatefulSink`/`StatefulSinkWriter`，把
sequenceCounter 与待 StreamLoad 的元数据（表名、label）写进状态；2PC 作为可选项」。

**审计先更正了一个前提**：此前记的"legacy 无状态 ⇒ 被 checkpoint 覆盖的缓冲行会丢"是**错的**。
Flink 1.18.1 的 `SinkWriterOperator.prepareSnapshotPreBarrier` 在**每个 checkpoint barrier 前无条件**调
`sinkWriter.flush(false)`，而 `flush(boolean)` 就是同步 PUT——被某个已完成 checkpoint 覆盖的每一行，在它完成
之前就已在 Doris 里。**legacy 的 1PC 路径没有丢数洞**，2PC 换来的是**可见性边界**，不是补一个洞。

**落地**（`sink.writer` 三档，默认 `legacy`，老 `DorisSink`/`DorisSinkWriter` 一行不改）：

| 档 | 类 | 语义 |
|---|---|---|
| `legacy` | `DorisSink`/`DorisSinkWriter` | 原样：一次 PUT 即一次提交（at-least-once + label 幂等） |
| `stateful` | `StatefulDorisSink`/`StatefulDorisSinkWriter` | 写路径同 legacy + 可 checkpoint 的 `sequenceCounter` |
| `stateful-2pc` | `TwoPhaseDorisSink`/`TwoPhaseDorisSinkWriter` + `DorisCommitter` | 预提交 + checkpoint 完成后提交 → **行在覆盖它的 checkpoint 完成前不可见** |

- **label 可推导**：`{前缀}_{db}_{table}_{subtask}_{epoch}_{rung}`，epoch = 该批所属 checkpoint id（严格递增）。
- **恢复不用问 Doris**：重启直接写回上次用过的 label，Doris 报出占着它的 txn → abort → 同 label 重载
  （F12 实测：abort 会释放 label）；撞上已 commit 的 label（FINISHED）则**响亮失败**并点名 `sink.label-prefix`。
- **状态里只有两项**：`sequenceCounter`（恢复取 `max(状态+1, 墙钟)`）与 `labelPrefix`（自描述）。
  **不记 in-flight 事务**：要恢复的那个 checkpoint 里开着的事务正是 committer 马上要提交的那批，writer 若在
  恢复时 abort 它们，重启后的 committer 会撞 "already aborted" 而永远起不来；**也不记缓冲行**（取状态时缓冲必空）。
- **协议细节实测定死**（doris-2.1.8，F1–F14，留档 `two-phase-commit-experiment.sql`）：`two_phase_commit` 头**两腿
  都要带**（FE 的 307 不是代理）、`_stream_load_2pc` 控制调用、四种幂等/失败措辞、2PC **不新增权限**。
- **已知边界**：2PC **必须开 checkpointing**（否则行永远不可见，ITCase 固化为断言）；预提交事务闲置
  3600s 会被 Doris 回收 → 长 checkpoint 间隔/背压会撞 commit 失败并重放。

**测试**：`mvn test`（本仓库的 `test` 阶段只跑 `*Test`）195 例全绿，其中本特性新增单测 38 例
（`DorisHttpClientTwoPhaseCommitTest` 12、`TwoPhaseDorisSinkWriterTest` 11、`StatefulDorisSinkWriterTest` 9、
`DorisCommitterTest` 6）；另有 `DorisTwoPhaseCommitITCase` 4 例（MiniCluster + 真实 sink 链，属 `integration-test`
阶段，本次单独 `-Dtest=DorisTwoPhaseCommitITCase` 跑通，重复三轮无 flaky）：1PC 不碰 2PC 端点 / 无 checkpoint
时行不可见 / 有 checkpoint 时恰好提交一次 / **故障重启清掉死掉那次尝试的事务**。

详见 [deep-dive/05](./deep-dive/05-doris-sink.md) §4、[deep-dive/01](./deep-dive/01-exactly-once.md) §6.1。

---

## P4 — Debezium（原低优先，2026-08-26 上提执行）

### P4-1 TiDB debezium 格式数据转换 ✅（随 S3 落地）

TiDB datetime 字面是几点、debezium 格式就是 UTC 多少（设置时区为 UTC）；TiCDC 把 decimal 处理为
`float64` 而非 `string`；Timestamp 是 `Instant`、Datetime 是 epoch millis——两个行为不同，转换器要区分。
详见 [deep-dive/02-message-parsing.md](./deep-dive/02-message-parsing.md) 的 Debezium 接入。

### P4-2 ticdc debezium schema change 支持 ⏸️

TiCDC 8.x **不支持** debezium 的 schema change 事件，9 开始支持（9.0 暂未发布）。测试路径：用
MySQL + Debezium 覆盖 schema change；等 ticdc 9.0 发布后再补 TiDB 侧。

---

## P5 — 性能 / 规模化（2026-10-23 排定）

> 背景：「单作业全库 `initial` 模式同步」瓶颈评估的落地项。量化基线：1 万表 / 100 亿行（~10 TB）/
> 快照 36h / 8 并行 / 默认 chunkSize 8096 → **约 123 万 split**。
> 前提说明：MySQL 路径 `KafkaJsonSourceConfig.isSkipSnapshotBackfill()` 已**强制跳过回填**，
> 评估中「回填读放大 ~5× 流量」对 MySQL 不适用；以下三项是快照期真正剩余的成本大头。

### P5-1 快照期 LW 水印探针复用 ⬜（性价比最高）

**问题**：MySQL 路径下 `KafkaJsonDialect.displayCurrentOffset` 每次调用都走
`KafkaJsonKafkaOffsetUtils.queryCurrentOffset`：**新建一个一次性 KafkaConsumer**（无 group、
用完即弃）→ `endOffsets` → 逐分区串行 `assign + seek + poll(3000ms 硬编码)`。快照期
**每个 split 探一次 LW**（MySQL 强制跳过回填后 HW=LW，不再探测）：

- 每次探针 ≈ consumer 建连+元数据（~200ms）+ 分区串行 poll（~300-800ms）≈ **0.5–1.5s**
- 123 万 split × ~1s / 8 并行 ≈ **40h+ 纯探针开销**；broker 侧还要承受 123 万次一次性连接 churn
- TiDB 路径无此问题（水印走 `SELECT TIDB_CURRENT_TSO()`，JDBC 毫秒级）

**方案**：把 `KafkaJsonOffsetSupplier`（`source/kafka/`，**已写好但未接线**——生产路径从未引用）
接入 `KafkaJsonDialect.displayCurrentOffset`，dialect 持有一个懒初始化、可复用的 consumer，
随 dialect 生命周期关闭。约束：`KafkaConsumer` 非线程安全，探针全部发生在 fetch task 线程，
复用实例要么绑定单线程、要么同步。进阶（可选）：同一 reader 的一批并发 split 共享一次探测
（min-LW 略变老，at-least-once 语义不受影响，重放窗口略宽）。

**验收**：单测断言 `displayCurrentOffset` 多次调用只建一个 consumer；ITCase 对比快照期 broker
连接数（1 次/split → 1 次/reader 量级）；快照总时长下降。

### P5-2 分块规划移出 enumerator 主线程 ⬜

**问题**：`JdbcSourceChunkSplitter.generateSplits` 在 `SnapshotSplitAssigner.getNext` 里**内联
执行**，而 `getNext` 跑在 enumerator 的 mailbox 主线程（`handleSplitRequest → assignSplits`）。
规划一张大表 = min/max + `information_schema.TABLE_ROWS` + 不均匀分块的逐 chunk 边界查询
（每 chunk 一条 + 每 10 条 sleep 100ms）——期间**整个作业的 split 分发停摆**（head-of-line
blocking）。1 万张表逐张串行规划，作业启动后的前几十分钟基本都耗在这。

**方案**：后台线程预生成（为表 N+1 规划的同时分发表 N 的 split），或逐表异步生成 + assigner 内
"splits pending"状态。约束：

- `SnapshotPendingSplitsState` 必须能在生成中途被 checkpoint（当前内存中的 pending 列表）
- 保持表的确定性顺序（分块顺序影响分发顺序与可复现性）
- 实现位置：`SnapshotSplitAssigner` 属 flink-cdc-base（与 MySQL 连接器共享）——优先做
  kafkajson 侧覆写/组合，避免改 base 语义

**验收**：mock 一个规划耗时数秒的大表，断言其后小表的 split 分发不被阻塞（秒级内）；现有
单测/ITCase 不回归。

### P5-3 快照完成后的协调者状态瘦身 ⬜（上游已修：FLINK-40697，本库走路线②连接器侧移植）

**问题**：`SnapshotSplitAssigner` 在 JobManager 内存维护**单调增长**的 `assignedSplits`（从不
收缩）、`splitFinishedOffsets`、`remainingSplits`、`tableSchemas`、`alreadyProcessedTables`；
快照结束后 `HybridPendingSplitsState` **永久**包裹这份状态 → **每次 checkpoint 全量重序列化**：

- 每 split 序列化 ~0.5–1.2KB（hex 编码 chunk 边界为主），一个 split 最多出现在状态里 3 处
- 123 万 split ≈ **0.6–1.5GB/次 checkpoint，终身背着**；36h 快照 × 30s interval ≈ 4.3TB 累计
  序列化流量
- 连锁反应：快照→流式的**阶段切换必须等一次成功 checkpoint**（notifyCheckpointComplete 翻转
  assigner 状态）→ 状态越大 checkpoint 越慢，切换被拖长甚至无法完成；另有 JM 堆/OOM 风险

**上游对齐（2026-10-26 调研）**：FLINK-40697 已把该修复泛化进 flink-cdc-base（MySQL 先行版为
FLINK-39775），机制与本项原方案一致，并补了四个设计细节：释放挂在「完成的 checkpoint」两步上
（arm → complete）、代数守卫防 failover reader 迟到上报、与 `scan.newly-added-table.enabled`
互斥 fail-fast、条件序列化器版本（默认关 = 老格式可恢复）。上游 patch 已存
`D:\Users\zhao.fulin\Desktop\4555.diff`——**基于 3.7.0 形状**（其序列化器 VERSION 8 vs 本库 6、
有本库不存在的 `splitFinishedCheckpointIds`），不能直接 apply，需手动适配。

**决策（2026-10-26，用户拍板）：base 保持原生不动**，即使官方 backport 也不进 base，走
**路线② = 连接器侧接管（MySQL 模式，同 FLINK-39775 先例）**：

| 部分 | 行数 | 性质 |
|---|---|---|
| `SnapshotSplitAssigner`(461)/`HybridSplitAssigner`(277)/状态类(181)/`PendingSplitsStateSerializer`(410) | ~1330 | **复制**进 kafkajson（assigner 的释放字段是 base 的 private final，子类够不着只能整份接管）；逐文件与 base diff 校验等价性（应只剩 package/import 差异） |
| 枚举器**子类**（base 类 public 可子类化，357 行不复制） | ~180 纯新 | 代数守卫 + 两步释放，对着 4555.diff 适配 3.2.1 形状 |
| 自有 assembled 事件类 + `KafkaJsonSourceReader` 子类钩子 + option 管线 | ~150 新 | reader 覆写 `handleSourceEvent` 检测收齐后上报（meta 事件 public 字段够用，base reader 零改动） |
| 测试 | ~300 | 从 4555.diff 的 release 测试适配 |

注入点均已核实：`createEnumerator`/`restoreEnumerator`/`getEnumeratorCheckpointSerializer` 可覆写，
`SplitAssigner` 是接口，条件版本号自定为 **6/7**（对应上游 8/9）。`KafkaJsonSplitSerializer` 的
字节等价校验法同样适用于复制部分。

**前置门槛（先做一行配置再决定投入）**：先调 `scan.incremental.snapshot.chunk.size`（8096 →
20 万+）观察真实规模——2 万 split ≈ 15–25MB/checkpoint 属可接受；**仍 GB 级再开工本项**。

**方案**：快照阶段完成 + 流式 split 已下发后，assigner 状态收缩为流式真正需要的最小集
（assignerStatus / `alreadyProcessedTables`（仅表名）/ 新增表发现所需的 `tableSchemas`），丢弃
`assignedSplits` / `splitFinishedOffsets` 的逐 split 明细。**正确性关键**：restore 路径
（`HybridSplitAssigner` 从 checkpoint 重建）必须靠收缩后的状态仍能得出「快照已全部完成、永远
不再下发快照 split」的结论（assignerStatus + isStreamSplitAssigned 足以判定），否则重启会
重发已完成的 split。TM 侧 stream split 状态（`finishedSnapshotSplitInfos` ~300B×split +
tableSchemas）是 `shouldEmit` 去重判据的一部分，**不在本项范围**（其压缩表示作为后续可选项）。

**验收**：快照结束后的 checkpoint 序列化字节数降到 MB 量级（断言）；故障重启用例——快照完成后
kill/restore → 不重发快照 split、流式衔接点不变。

### P5-4 流式阶段按分区拆 split ⏸️（不适配当前环境）

流式 split 的有效并行度 = 1（单消费者、单线程 manual assign 全部分区）是架构天花板，按 Kafka
分区拆 stream split 是根本解。**当前全部 topic 为单分区，拆分无收益**，维持现状；topic 分区数
变化后重估。

---

## 建议执行顺序

1. **P0** 三项互相纠缠（边界、去重、并发模型），已按一个工作单元推进完毕。
2. **P1** 转换正确性 + **P2** TiDB 链路已落地。
3. **P3-1** 时钟工具类（待办）可与下一步 DDL 扩展一起做。
4. **P4** Debezium 已上提执行（S1-S4 全绿）；TiDB 侧 DDL 等 ticdc 9.0。
5. **P5** 性能/规模化按 P5-1 → P5-2 → P5-3 排期：P5-1 性价比最高、改动最小；P5-2 动 base
   前先评估 kafkajson 侧覆写的可行性；P5-3 状态收益立竿见影但正确性敏感，需完整的故障重启
   验收。P5-4 环境不适用，维持 ⏸️。

> 依赖注记：P1-1 与 P1-2 同为转换正确性；P2-1 是 P2-2 的前置调研点（已完成）。
