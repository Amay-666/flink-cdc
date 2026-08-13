# 流式边界语义审计报告

> 审计对象：`flink-connector-jdbc-kafka-json-cdc`（下称**连接器**）vs `flink-cdc-base`（下称**基座**，3.2.1，released/不可改）
> 审计方式：代码逐行核对（两端源码）+ 与 ROADMAP / EXACTLY_ONCE 文档的历史结论交叉验证
> 审计日期：2026-08-13
> 结论速览：**连接器的回填边界语义与基座 pure-stream 阶段的含端点阈值在 `es == max` 精确边界处重叠，真实场景（快照期间 topic 非空）下该记录被双发**；现有全部 ITCase 因"快照期空 topic"刻意绕开了此路径。另发现死代码与文档漂移各一处。
> **修复状态（2026-08-13）**：F1 的 Tier 1（流式 split 排他下界）已实现并附回归测试（见 §8）；Tier 2（多 split 排他上界）为未实现的后续项；F2（死代码）、F3（文档漂移）为独立清理项，本次未做。

---

## 0. 结论摘要（TL;DR）

| # | 级别 | 结论 |
|---|---|---|
| F1 | **已修复（Tier 1，见 §8）** | 当水位取自 **Kafka 采样**（MySQL+es、MySQL+ts、TiDB+ts）且快照期间 topic 非空时，HIGH 捕获时刻"最新"的那条消息（`es == max`，sentinel 排序保证其不越过回填上界）会被**有界回填发出一次**，又被**流式 pure-stream 阶段（基座 `isAtOrAfter` 含端点阈值）再放行一次** → 双发。这是常态，不是边角。修复后：单 split 下由流式排他下界闭合；多 split 的 `es == 非最小 HIGH` 边界残留由 Tier 2 覆盖（未实现）。 |
| F2 | **确认（代码级）** | `KafkaJsonOffsetSupplier` 为生产路径死代码：`KafkaJsonSourceFetchTaskContext.getOffsetSupplier()` 无任何生产调用。P0-4 只更新了它的 javadoc，类本身已退化为 `queryCurrentOffset` 的空壳包装。 |
| F3 | **确认（代码级）** | 文档漂移：`EXACTLY_ONCE.md` §3 写回填 `(LOW, HIGH]`（下界已过时，实际 `[LOW, HIGH]`）；ROADMAP P0-1"结论"描述的排他上界语义已被 P0-4 取代；P0-4 声称"流式发 `(HIGH, …)`"与实际代码（基座含端点 pure-stream）不符。 |
| F4 | **理论（需针对性测试证伪/证实）** | 多分区/多 split + `es` 乱序（跨分区发布滞后）时，pure-stream 一旦被触发就绕过 per-split 过滤，晚到的 `es ≤ 阈值` 记录（已被回填或已被 JDBC 快照覆盖）会再被放行 → 双发窗口扩大。 |
| F5 | **确认（代码级）** | **TiDB + es（TSO 水位）是干净的**：HIGH=TSO 落在消息之间，无消息 `es == TSO`，流式 `isAfter(TSO)` 天然过滤。这是四种 `(databaseType, eventTime)` 组合中唯一不受 F1 影响的。 |

影响面：**只要快照期间 Kafka 有消息（生产常态），MySQL 全系 + TiDB+ts 的快照→增量切换在精确边界处不满足 exactly-once**；F1 的双发在幂等 upsert 型下游不可见，在 INSERT/非幂等/DELETE 场景可见。基座 MySQL 连接器无此问题（binlog 字节位置天然落在事件之间）。

---

## 1. 范围与方法

**范围**：只读不改（审计）。核对文件——连接器侧：
- `KafkaJsonDialect`（`displayCurrentOffset` 分支、`createFetchTask`、`notifyCheckpointComplete`）
- `KafkaJsonScanFetchTask` / `KafkaJsonSourceFetchTaskContext` / `KafkaJsonStreamFetchTask`
- `KafkaJsonKafkaOffsetUtils.queryCurrentOffset`（MAX 语义）
- `KafkaJsonOffset`（哨兵与排序）
- `KafkaJsonTidbOffsetUtils`（TSO 边界）

基座侧（released，仅核对）：
- `AbstractScanFetchTask.execute` / `createBackfillStreamSplit` / `dispatch*WaterMarkEvent`
- `IncrementalSourceStreamFetcher.shouldEmit` / `hasEnterPureStreamPhase` / `configureFilter`
- `HybridSplitAssigner.createStreamSplit`
- `IncrementalSourceRecordEmitter`、`SnapshotSplitState` / `StreamSplitState`

**方法**：① 读两端源码，把连接器实现的每处边界判断与基座调用点一一对应；② 用 ROADMAP P0-1/P0-4 的历史结论做"设计意图 vs 实际代码"的对照；③ 逐条核实 ITCase 是否覆盖对应路径。

---

## 2. 依赖：连接器如何嵌入基座

连接器在基座 FLIP-27 增量快照框架里做四类插件化替换（全部通过 `JdbcDataSourceDialect` 接口、`JdbcIncrementalSource` 驱动），不改基座：

| 基座调用点 | 连接器实现 | 职责 |
|---|---|---|
| `dialect.displayCurrentOffset()`（LOW/HIGH 捕获） | `KafkaJsonDialect#displayCurrentOffset` | 快照分片的双水位来源。TiDB+es → `queryCurrentOffset`(TSO)；其余 → `queryCurrentOffset`(Kafka 采样) |
| `dialect.createFetchTask(split)` | `KafkaJsonScanFetchTask`（快照）／`KafkaJsonStreamFetchTask`（流式/回填） | 快照读 MySQL（JDBC）；流式与回填读 Kafka |
| `dialect.createFetchTaskContext()` | `KafkaJsonSourceFetchTaskContext` | 每 subtask 一份：recordConverter / recordFactory / schema / 懒加载 Kafka consumer / JDBC |
| `dialect.createChunkSplitter()` | `KafkaJsonChunkSplitter` | 快照分片 |
| `offsetFactory` | `KafkaJsonOffsetFactory` | `INITIAL_OFFSET` / `NO_STOPPING_OFFSET` 哨兵 |
| `dialect.queryTableSchema()` | `KafkaJsonSchema`（volatile+synchronized 缓存） | 分片拆分与流式 schema 发现共用 |
| `dialect.notifyCheckpointComplete()` | → `KafkaJsonStreamFetchTask.commitCurrentOffset` | Kafka group offset **不提交**，只依赖 Flink checkpoint |

**共享实例约束**：`dataSourceDialect` 是跨 subtask 单实例（P0-3 已处理其并发）；`streamFetchTask` 是 dialect 字段，`notifyCheckpointComplete` 与 stream read 共用（单 stream split，天然串行）。

---

## 3. 类执行流程（时序）

### 3.1 快照分片（`AbstractScanFetchTask.execute`，基座 L48-116）

对每个 snapshot split（可跨 subtask 并行）：

```
1. LOW  = displayCurrentOffset()                     → 派发 LOW watermark
2. JDBC 读分片数据（KafkaJsonSnapshotSplitReadTask，行转 READ 记录入队）
3. HIGH = displayCurrentOffset()                     → 派发 HIGH watermark
   (skipSnapshotBackfill=true 时 HIGH=LOW，退化为 at-least-once)
4. streamBackfillRequired = HIGH.isAfter(LOW) ?
     false → 基座直接 dispatchEndWatermark（低==高，无回填窗口）
     true  → 连接器 executeBackfillTask(createBackfillStreamSplit(LOW, HIGH))
5. END watermark 归属：
   - 无回填：基座 AbstractScanFetchTask 派发
   - 有回填：连接器 KafkaJsonStreamFetchTask 在有界读结束时派发   ← P0-1 已核实为"不是 bug"
6. 派发 HIGH 时 IncrementalSourceRecordEmitter.setHighWatermark → SnapshotSplitState
   → 分片 finished，finished offset = HIGH
```

### 3.2 回填（`KafkaJsonScanFetchTask.executeBackfillTask` → `new KafkaJsonStreamFetchTask(backfillSplit).execute`）

- backfill split = `StreamSplit(startingOffset=LOW, endingOffset=HIGH)`（基座 `createBackfillStreamSplit` L118-126）。
- **复用同一 subtask 的共享 Kafka consumer**（`getKafkaConsumer`），`isBoundedRead()==true`。
- 有界读：`partitionEndOffsets = consumer.endOffsets(...)`；`processRecords` 逐条做边界判断（见 §4.2）；**所有已分配分区**都越过 ending 或排空到 log end 才 `dispatchEndWatermark` 并结束（基座 stream fetcher 收到任务结束 → `isFinished()`）。

### 3.3 主 stream（`HybridSplitAssigner.createStreamSplit` → `IncrementalSourceStreamFetcher.submitTask`）

- 起点：`minOffset = min(所有 finished snapshot split 的 HIGH)`（`createStreamSplit` L237-243、L271）；无快照 split 时回退 `createInitialOffset()`。
- 终点：`createNoStoppingOffset()`（snapshot-only 模式为 `maxOffset`，L261-264）。
- **基座 `IncrementalSourceStreamFetcher` 自身不派发 stream 阶段 END**（`submitTask` L80-105 只 `execute → finally stopReadTask`）；连接器 stream 任务在有界模式（snapshot-only）下自己 `dispatchEndWatermark`，无界模式永不结束。
- `shouldEmit`（基座 L178-203）在 `pollSplitRecords` 时对每条 DataChangeEvent 过滤（详见 §4.3）。

### 3.4 checkpoint / restore

- 每记录：`IncrementalSourceRecordEmitter.processElement → updateStreamSplitState.setStartingOffset(position)` → StreamSplitState 持续前移。
- HIGH watermark → `SnapshotSplitState.setHighWatermark`。
- `StreamSplitState.toSourceSplit()` 重建 split（含最新 startingOffset），`SourceSplitSerializer`(V5) / `PendingSplitsStateSerializer`(V6) 序列化。
- 恢复后从已提交 checkpoint 的 split 进度 + stream 起点续读（Flink checkpoint 对齐保证）。

---

## 4. 边界语义精确盘点（现状，已逐行核实）

### 4.1 Offset 模型与哨兵

`KafkaJsonOffset = (eventTime, partition, offset)`，排序 `eventTime → partition → offset`。eventTime 取自**消息内容**的 `es`/`ts`（不是 Kafka 记录时间戳）。

- `INITIAL_OFFSET = (-1, -1, -1)`
- `NO_STOPPING_OFFSET = (Long.MAX_VALUE, -1, -1)`
- 水位哨兵：`(es, Integer.MAX_VALUE, Long.MAX_VALUE)`。真实消息 partition < MAX，故**与水位同 es 的消息排在水位之前**（`isAfter(哨兵)=false`）→ 归属回填（含端点）。

### 4.2 有界回填窗口 = `[LOW, HIGH]`（含端点，实际代码）

`KafkaJsonStreamFetchTask.processRecords`（L187-282）：
- **下界**（L226）：`es < startingOffset.es → drop`（含端点的下界；快照前变更的效果已在 JDBC 快照里）。
- **上界**（L233）：`lastOffset.isAfter(endingOffset) → drop + 标记该分区越过 ending`（**含端点**：`es == ending.es` 的消息因 partition < MAX 不触发 `isAfter`）。
- 终止（L273-278）：`partitionsPastEnding.contains(p) || consumer.position(p) >= endOffset(p)`（空分区 end==0 视为已排空，避免挂死）。

### 4.3 流式起点与过滤

- **seek**（`assignAndSeek` L318-335）：`startingOffset.es > 0 → consumer.offsetsForTimes({p → startingOffset.es})`，seek 到每个分区"首个**记录时间戳** ≥ startingOffset.es"的 offset；无此消息的分区 `seekToEnd`。`es ≤ 0` → 按 startup mode 兜底 seek。
- **连接器流式任务排他下界**（修复后，`KafkaJsonStreamFetchTask.processRecords` L211-223）：流式 split 入队前丢弃 `lastOffset.isAtOrBefore(startingOffset)`，流式实际只读 `> startingOffset`（§8）。基座 pure-stream 的含端点阈值因此对 `es == HIGH` 边界消息失效，该消息只由回填发出一次。
- **shouldEmit**（基座 L178-225）：
  1. **pure-stream 阶段**（L205-225）：`pureStreamPhaseTables.contains(tableId)` **或** `position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))` → 加入集合并**放行**。阈值**含端点**（`isAtOrAfter`）。
  2. per-split 分支（L186-196）：`isRecordBetween(splitStart, splitEnd) && position.isAfter(split.getHighWatermark())` → 放行。
  3. 否则 drop。
- `maxSplitHighWatermarkMap`（`configureFilter` L247-251）= 每表已 finished split 的 HIGH 的最大值。

### 4.4 水位来源（`queryCurrentOffset` MAX 语义）

`KafkaJsonKafkaOffsetUtils.queryCurrentOffset`：读每个分区最新一条消息（`endOffset-1`），从**消息内容**取 es/ts，返回**最大** `(maxEs, Integer.MAX_VALUE, Long.MAX_VALUE)`；topic 空 → `INITIAL_OFFSET`。TiDB+es 改用 `KafkaJsonTidbOffsetUtils.queryCurrentOffset`（TSO）。

---

## 5. 审计发现

### F1（确认）——精确边界 `es == max` 双发

**触发条件**（全部满足才复现）：
1. 水位取自 Kafka 采样：`(MySQL, es)`、`(MySQL, ts)`、`(TiDB, ts)` —— 排除 TiDB+es（TSO）；
2. 快照期间 topic 非空（**生产常态**：canal/TiCDC 持续写入）；
3. 单分区即复现，无需多分区。

**机制**（代码级推导，全部核实行号）：
1. HIGH 捕获时 `max = max(各分区最新消息 es)`，存在至少一条消息 `B`，`B.es == max`（最新那条，某个分区上）。
2. **回填**：ending 哨兵 `(max, MAX, MAX)`；`B=(max, p, o)`，`p < MAX → !isAfter → B 被回填发出`（`KafkaJsonStreamFetchTask` L219）。
3. **流式**：`startingOffset = min(各 split HIGH) = (max, MAX, MAX)`（单 split 时）。`offsetsForTimes(max)` seek 到"首个记录时间戳 ≥ max"；`B.recordTimestamp ≥ B.es == max` → **B 在流式读取范围内**（`assignAndSeek` L304-321）。
4. **shouldEmit(B)**：基座 pure-stream 判定 `position.isAtOrAfter(maxSplitHighWatermark)`（`IncrementalSourceStreamFetcher` L211）——`B.es == max` → **触发 pure-stream → 放行**。per-split 分支（`isAfter(HIGH)`）本来会拦下 B（`es==HIGH` 不 after），但 pure-stream 在它之前短路放行。
5. → **B 由回填与流式各发出一次 = 双发**。

**为什么现有测试全绿**：4 个 Phase-11 ITCase 都刻意"先把增量消息写入 Kafka 缓冲、或快照完成后再写"——快照开始时 topic 为空 → `HIGH = INITIAL_OFFSET(es=-1)` → 回填窗口为空、流式走 startup-mode 兜底 seek，**从不出现 `es == max` 的消息**（`KafkaJsonSimulatedChainITCase` javadoc 明说 "empty topic at snapshot start → high watermark -1"）。单测 `KafkaJsonStreamFetchTaskTest` 只覆盖回填侧含端点的保留/下界丢弃，不覆盖流式重读该记录。

**与历史结论的关系**（ROADMAP 对照）：
- P0-1（排他上界 `[LOW, HIGH)`）：`es==max` 消息**不**回填，由流式 pure-stream 单独发出。是否重复取决于 B 是否在 JDBC 读完成前落库（落库 → 与快照重复；未落库 → 必要且正确）。
- P0-4（含端点上界 `[LOW, HIGH]`）：把 `(min, 真实边界]` 内全部记录交给回填（按主键与快照去重，**正确**），但**没有**让流式排除 `es == max` —— 流式 pure-stream 的含端点阈值仍放行它 → **确定性的双发**（较 P0-1 的"有时重复"更坏）。P0-4 声称"流式发 `(HIGH, …)`"与基座 `isAtOrAfter` 不符。

**修复**（2026-08-13 已实现）：流式任务在 `processRecords` 加一条与回填上界对称的下界 `lastOffset.isAtOrBefore(startingOffset) → drop`（即流式严格 `> startingOffset`），让含端点阈值失效、`es == max` 只由回填发一次。单 split（并行度 1）下完全闭合；多 split 的 `es == 非最小 HIGH` 边界残留见 §8 Tier 2（后续项）。

### F2（确认）——`KafkaJsonOffsetSupplier` 死代码

- `KafkaJsonSourceFetchTaskContext.getOffsetSupplier()`（L192-194）**无任何生产调用**（全仓 grep 仅定义处）。
- P0-4 把 `queryCurrentOffset` 改为 MAX 后只更新了该类的 javadoc；类体已退化为 `queryCurrentOffset` 的薄包装，且持有**从未被使用**的独立 `KafkaConsumer`（每构造一个就多一个连接）。建议删除或挂到 `displayCurrentOffset` 复用。

### F3（确认）——文档漂移

| 位置 | 现状 | 实际 |
|---|---|---|
| `EXACTLY_ONCE.md` §3 step 4 | 回填 `(LOW, HIGH]` | `[LOW, HIGH]`（下界含端点，P0-4 后） |
| `EXACTLY_ONCE.md` §4 | "每个键在管线内只发射一次" | 对 `es==max` 不成立（F1） |
| `ROADMAP.md` P0-1"结论" | 排他上界 `[LOW, HIGH)` | 已被 P0-4 含端点上界取代 |
| `ROADMAP.md` P0-4 | "流式发 `(HIGH, …)`" | 基座 pure-stream 是 `isAtOrAfter`（含端点），流式实际 `[HIGH, …)` |

### F4（理论，需针对性测试）——pure-stream 绕过 per-split 过滤的双发窗口

同一根因的放大形态：一旦 `es ≥ max` 的任一记录触发 pure-stream，该表**后续全部记录**直接放行（基座 L205-207、L211-213），per-split 的 `isAfter(HIGH_i)` 过滤失效。若某条记录满足：
- `es ≤ max`（已被回填，或 `es < LOW` 效果已被快照覆盖），**且**其记录时间戳 ≥ 触发记录的时间戳（跨分区发布滞后 / canal 乱序），被读到 → 重复。

单分区下 `es` 与记录时间戳均单调，此形态不出现（只剩 F1 的 `es == max` 一条）；**多分区**下热分区先发 `es==max` 消息、冷分区 `es < max` 消息晚到，即可触发。此形态不在任何测试覆盖内，也不能靠现有 ITCase 证伪。

> **审计修正（2026-08-13）**：初稿曾假设"多 split 并行时各 split 的回填窗口相互重叠 → 同一记录可能被多个 split 的回填发出（源级重复）"。逐行核对基座 `IncrementalSourceScanFetcher` 后排除：scan fetcher 对每条回填记录按 chunk key-range 过滤（`isChangeRecordInChunkRange`，L239-247），同一记录只可能被**包含其 key** 的那一个 split 回填 → 源级不重复，此维度不再是 F4 的组成部分。F4 剩下的内容就是本条前半的 pure-stream 短路对晚到 `es ≤ HIGH` 记录的放行——与 F1 同根，由修复方案的 Tier 1/2 一并闭合。

### F5（确认）——TiDB+es（TSO）免疫

HIGH = TSO（DB 提交时钟），`es` 是提交时间戳；**不存在 `es == TSO` 的消息**（TSO 是查询时刻，消息已提交的 `es` 恒 `< TSO`）。流式 `offsetsForTimes(TSO)` 读到 `es < TSO` 的记录时：pure-stream 不触发（`es < TSO`）、per-split `isAfter(TSO)` 为 false → 全部过滤 → 不双发、不丢失。TSO 边界在语义上等价于 MySQL 的 binlog 字节位置——**落在消息之间**。这是四组合里唯一干净的一档，恰好也是本连接器 TiDB 链路默认配置（`databaseType=tidb, event-time=es`）。

---

## 6. 影响面评估

| `(databaseType, event-time)` | 水位来源 | F1 | F4 | 备注 |
|---|---|---|---|---|
| MySQL + es（默认） | Kafka 采样 MAX | **命中** | 可能 | 生产常态双发 |
| MySQL + ts | Kafka 采样 MAX | **命中** | 可能 | 同上 |
| TiDB + ts | Kafka 采样 MAX | **命中** | 可能 | 同上 |
| TiDB + es | TSO | **免疫** | 免疫 | 干净 |
| snapshot-only | 同上 | 见 P0-1 已知边角 | — | 终点 `maxOffset` 有界 |

下游可见性：CREATE/UPDATE 在幂等 upsert 型 sink（StarRocks 等）不可见；INSERT 新行、DELETE、或非幂等/计数型消费端可见重复。

> 修复后（§8 Tier 1）：单 split 下三档 Kafka 采样组合的 F1 已闭合；多 split 的边界残留由 Tier 2（未实现）覆盖；TiDB+es 始终免疫。

---

## 7. 验证建议（针对性回归）

1. **F1 定向回归（最高优先）**：`KafkaJsonStreamFetchTaskTest` 级，用 `FakeKafkaConsumer` 构造"快照 HIGH 捕获后 topic 非空"：预置消息含 `es == HIGH.max`，模拟单 split 快照 → 断言该消息在**回填**出现一次、**流式重读**被丢弃。即 P0-4 回归的正交补集（P0-4 只验证了回填保留边界消息，未验证流式不重放它）。**已完成**：`testStreamSplitDropsBoundaryMessageAtStartingOffset`（见 §8）。
2. **F4 定向回归**：两分区，分区 A 最新消息 `es=max`（先发），分区 B 一条 `es=max-1000` 后发（记录时间戳更晚）；断言 B 分区这条不被流式再发（或被回填恰一次）。
3. **文档订正**：EXACTLY_ONCE §3 下界、§4 恰好一次表述；ROADMAP P0-1 标注"已由 P0-4 取代"、P0-4 标注"`es==max` 残留重复（F1）"。
4. **死代码清理**：删除 `KafkaJsonOffsetSupplier` + `getOffsetSupplier()`（或挂到 `displayCurrentOffset` 复用，删除独立 consumer）。

---

## 8. 修复记录（2026-08-13）

### F1 Tier 1（已实现）：流式 split 排他下界

**改动**：`KafkaJsonStreamFetchTask.processRecords`，在 `lastOffset` 计算之后、DDL/数据入队之前新增一条守卫：

```java
if (StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
        && lastOffset.isAtOrBefore(startingOffset)) {
    // ... 流式 split 严格读 > startingOffset；回填 split 不受影响
    continue;
}
```

- **触发面**：仅流式 split（`splitId == STREAM_SPLIT_ID`）；回填 split 的 `splitId` 是快照分片 id（基座 `createBackfillStreamSplit`），保持含端点 `[LOW, HIGH]`，必须继续发 `es == HIGH` 边界消息。
- **语义**：流式阶段从"含端点（基座 `isAtOrAfter`）"收窄为"严格 `> startingOffset`"。因为水位是 `(es, MAX, MAX)` 哨兵，任何 `es == startingOffset.es` 的真实消息（partition < MAX）都满足 `isAtOrBefore` → 被丢弃。
- **无损性**：`es ≤ startingOffset` 的记录，要么被回填 `[LOW, HIGH]`（含端点）经基座 scan fetcher 按键合并恰一次发出（`rewriteOutputBuffer`，已核实**只覆盖、不丢弃**），要么 `es < LOW` 效果已在 JDBC 快照里 → 流式丢弃不丢数据；`es > startingOffset` 全部保留。被丢弃的边界消息仍推进 consumer 位置与 `currentOffset`（checkpoint 进度不倒退）。
- **覆盖**：单 split（并行度 1）下 MySQL+es/ts、TiDB+ts 的 F1 完全闭合；TiDB+es（TSO）本就免疫，无副作用。

**测试**（`KafkaJsonStreamFetchTaskTest`）：
- 新增 `testStreamSplitDropsBoundaryMessageAtStartingOffset`：边界消息 `es == startingOffset (3000,MAX,MAX)` 被丢弃（`currentOffset` 仍推进到该消息、position 正常前移），严格在其后的 UPDATE 正常发出 —— F1 的正交回归（回填侧保留边界已由 `testBoundedReadKeepsBoundaryMessageAtEndingOffset` 覆盖）。
- 原四个"有界回填"单测改用新增的 `backfillSplit` 帮助方法（`splitId = "snapshot-split-0"`），保留含端点回填语义 —— 避免被流式排他下界误伤。
- `testConsumesConvertsAndTracksOffset` 的流式起始水位下移至首条消息之前（2999），消除边界歧义。

### F1 Tier 2（未实现，后续项）：多 split 排他上界

多 split（并行度 > 1 或大表分 chunk）时 `startingOffset = min(各 split HIGH)`，`es == 非最小 HIGH` 的边界消息仍会被基座 pure-stream 的 `isAtOrAfter` 放行一次（与所属 split 的回填双发）。修法：流式任务入队前按 `split.getFinishedSnapshotSplitInfos()` 逐条核对 —— `isRecordBetween(record, splitStart, splitEnd) && position.isAtOrBefore(splitHigh)` → drop。复用基座 `JdbcSourceFetchTaskContext.isRecordBetween` 与 `FinishedSnapshotSplitInfo`（基座 `configureFilter` 同源数据）。该方案一并闭合 F4 的"晚到 `es ≤ 阈值` 记录被 pure-stream 放行"分支。

### 本次未做（独立清理项）

- **F2**：删除死代码 `KafkaJsonOffsetSupplier` + `getOffsetSupplier()`。
- **F3**：订正 `EXACTLY_ONCE.md` §3 下界 `[LOW, HIGH]`、§4 exactly-once 表述；`ROADMAP.md` P0-1/P0-4 标注 F1 残留。

---

## 9. 附录：关键代码引用

| 位置 | 内容 |
|---|---|
| `KafkaJsonStreamFetchTask.java` L211-223 | 流式 split 排他下界（修复）：`STREAM_SPLIT_ID && lastOffset.isAtOrBefore(startingOffset)` → drop |
| `KafkaJsonStreamFetchTask.java` L225-243 | 回填窗口 `[LOW, HIGH]`：下界 L226 `es < starting`、上界 L233 `isAfter(ending)`（含端点） |
| `KafkaJsonStreamFetchTask.java` L273-278 | 有界读终止：全分区越过 ending 或排空 |
| `KafkaJsonStreamFetchTask.java` L318-335 | `offsetsForTimes(starting.es)` seek（按**记录时间戳**，非 es） |
| `KafkaJsonStreamFetchTask.java` L353-365 | `dispatchEndWatermark(endingOffset, END)` |
| `IncrementalSourceStreamFetcher.java` L178-203 | `shouldEmit`：pure-stream 短路放行 |
| `IncrementalSourceStreamFetcher.java` L205-225 | `hasEnterPureStreamPhase`：**L211 `isAtOrAfter`（含端点）** |
| `IncrementalSourceStreamFetcher.java` L227-256 | `configureFilter`：`maxSplitHighWatermarkMap` = 每表 max HIGH |
| `AbstractScanFetchTask.java` L48-116 | LOW→读→HIGH→(回填|直派 END)；L83-86 skip 时 HIGH=LOW；L118-126 建回填 split |
| `HybridSplitAssigner.java` L228-276 | 主 stream：起点 = min(各 split HIGH) L237-243/L271；终点 NO_STOPPING L261-264 |
| `KafkaJsonKafkaOffsetUtils.queryCurrentOffset` | MAX 语义 `(maxEs, MAX, MAX)`；空 topic → INITIAL |
| `KafkaJsonOffset` | 哨兵排序：消息 partition < MAX → 同 es 排在水位前 |
| `KafkaJsonSourceFetchTaskContext.getOffsetSupplier` L192-194 | 死代码（无生产调用） |
| `KafkaJsonSimulatedChainITCase.java` L44-56、L106-115 | 测试前提"空 topic 快照 → HIGH=-1"，绕开 F1 路径 |
