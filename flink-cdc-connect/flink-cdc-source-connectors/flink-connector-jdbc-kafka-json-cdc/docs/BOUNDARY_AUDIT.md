# 流式边界语义审计报告

> 审计对象：`flink-connector-jdbc-kafka-json-cdc`（下称**连接器**）vs `flink-cdc-base`（下称**基座**，3.2.1，released/不可改）
> 审计方式：代码逐行核对（两端源码）+ 与 ROADMAP / EXACTLY_ONCE 文档的历史结论交叉验证
> 审计日期：2026-08-13（2026-08-14 修正 F1/F4 机制结论）
> 结论速览：初稿判定的 F1（`es == max` 精确边界双发）**经实证不成立**——水位哨兵 `(es, MAX, MAX)` 的 `partition=MAX` 使基座 pure-stream 的 `isAtOrAfter` 对真实消息**等效排他**，边界消息在单 split 单分区下被基座 per-split 分支正常丢弃。真正存在的是 **F4（pure-stream 短路放行）**：多分区轮询下，一条 `es > max` 的记录先被读到会触发 pure-stream，之后该表**所有**记录（含已被回填的 `es ≤ 所属 HIGH` 记录）被基座无条件放行 → 双发。Tier 1（流式排他下界）闭合单 split 的 F4 泄漏，Tier 2（按 finished split 预过滤）闭合多 split 的 F4 泄漏，均已实现并附回归测试（见 §8）。
> **修复状态（2026-08-14）**：F1 结论已修正；F4 为真实缺陷，其单 split 形态由 Tier 1 闭合、多 split 形态由 Tier 2 闭合（§8 全部已实现 + 回归测试）；F2（死代码）、F3（文档漂移）为独立清理项，本次未做。

---

## 0. 结论摘要（TL;DR）

| # | 级别 | 结论 |
|---|---|---|
| F1 | **已证伪（初稿判定撤销）** | 初稿断言"`es == max` 边界消息在回填发出一次、又被 pure-stream 含端点阈值放行一次 → 确定性双发"。实证（OrderProbe，真实类）与代码逐行核对均否定：pure-stream 阈值虽写成 `isAtOrAfter(maxHigh)`，但 maxHigh 是 `(max, MAX, MAX)` 哨兵，真实消息 `partition < MAX` → `es == max` 的消息**不**触发 pure-stream；per-split `isAfter(HIGH)` 同样为 false → 边界消息被基座丢弃。**单 split 单分区本就 exactly-once，无需修复**。真正的缺陷是同一根因的 F4。 |
| F2 | **确认（代码级）** | `KafkaJsonOffsetSupplier` 为生产路径死代码：`KafkaJsonSourceFetchTaskContext.getOffsetSupplier()` 无任何生产调用。P0-4 只更新了它的 javadoc，类本身已退化为 `queryCurrentOffset` 的空壳包装。 |
| F3 | **确认（代码级）** | 文档漂移：`EXACTLY_ONCE.md` §3 写回填 `(LOW, HIGH]`（下界已过时，实际 `[LOW, HIGH]`）；ROADMAP P0-1"结论"描述的排他上界语义已被 P0-4 取代；P0-4 声称"流式发 `(HIGH, …)`"与实际代码不符（见 §5 F1 修正）。 |
| F4 | **确认（真实缺陷，Tier 1+2 已闭合）** | 一旦某表任一记录 `es ≥ maxHigh` 触发 pure-stream，基座 `shouldEmit` 对**该表后续全部记录**短路放行（`hasEnterPureStreamPhase` L205-225）。多分区轮询下，热分区先读到 `es > max` 的触发记录、冷分区后读到**已被回填**的 `es ≤ 所属 HIGH` 记录 → 该记录被二次发出。单 split 形态由 Tier 1 闭合；多 split 的 `es ∈ (min HIGH, 所属 HIGH]` 残留由 Tier 2 闭合。 |
| F5 | **确认（代码级）** | **TiDB + es（TSO 水位）是干净的**：HIGH=TSO 落在消息之间，无消息 `es == TSO`，流式 `isAfter(TSO)` 天然过滤。这是四种 `(databaseType, eventTime)` 组合中唯一天然免疫的。 |

影响面（修正后）：**单分区 / 单 split 下连接器本就 exactly-once**（基座 per-split 过滤在偏移有序读取下正确丢弃已回填记录）。双发只出现在**多分区且跨分区乱序**（触发记录先于已回填记录被读到）时：单 split 由 Tier 1 闭合，多 split 由 Tier 2 闭合。F4 的双发在幂等 upsert 型下游不可见，在 INSERT 新行 / DELETE / 非幂等或计数型消费端可见。

---

## 1. 范围与方法

**范围**：只读不改（审计）+ 连接器侧修复。核对文件——连接器侧：
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

**方法**：① 读两端源码，把连接器实现的每处边界判断与基座调用点一一对应；② 用 ROADMAP P0-1/P0-4 的历史结论做"设计意图 vs 实际代码"的对照；③ 用真实类的 OrderProbe 程序实证哨兵排序下的边界判定；④ 逐条核实 ITCase 是否覆盖对应路径。

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
- 水位哨兵：`(es, Integer.MAX_VALUE, Long.MAX_VALUE)`。真实消息 partition < MAX，故**与水位同 es 的消息排在水位之前**（`isAfter(哨兵)=false`、`isAtOrAfter(哨兵)=false`）→ 归属回填（含端点）；同 es 的消息也**不会**越过哨兵触发 pure-stream。

### 4.2 有界回填窗口 = `[LOW, HIGH]`（含端点，实际代码）

`KafkaJsonStreamFetchTask.processRecords`：
- **下界**：`es < startingOffset.es → drop`（含端点的下界；快照前变更的效果已在 JDBC 快照里）。
- **上界**：`lastOffset.isAfter(endingOffset) → drop + 标记该分区越过 ending`（**含端点**：`es == ending.es` 的消息因 partition < MAX 不触发 `isAfter`）。
- 终止：`partitionsPastEnding.contains(p) || consumer.position(p) >= endOffset(p)`（空分区 end==0 视为已排空，避免挂死）。

### 4.3 流式起点与过滤

- **seek**（`assignAndSeek`）：`startingOffset.es > 0 → consumer.offsetsForTimes({p → startingOffset.es})`，seek 到每个分区"首个**记录时间戳** ≥ startingOffset.es"的 offset；无此消息的分区 `seekToEnd`。`es ≤ 0` → 按 startup mode 兜底 seek。
- **连接器流式任务排他下界（Tier 1）**：流式 split 入队前丢弃 `lastOffset.isAtOrBefore(startingOffset)`，流式实际只读 `> startingOffset`（§8）。这使得"已被回填、但会在基座 pure-stream 触发后再次被放行"的 `es ≤ min HIGH` 记录在入队前即被丢弃。
- **连接器流式任务多 split 预过滤（Tier 2）**：流式 split 入队前按 `split.getFinishedSnapshotSplitInfos()` 逐条核对 `es ≤ 所属 split HIGH && isRecordBetween(所属范围)` → drop（§8）。覆盖 `es ∈ (min HIGH, 所属 HIGH]` 的 F4 残留。
- **shouldEmit**（基座 L178-225）：
  1. **pure-stream 阶段**（L205-225）：`pureStreamPhaseTables.contains(tableId)` **或** `position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))` → 加入集合并**放行**。⚠️ **修正**：虽写成 `isAtOrAfter`（比较层含端点），但 `maxSplitHighWatermark` 是 `(max, MAX, MAX)` 哨兵，真实消息 `es == max` 时 `partition < MAX` → `isAtOrAfter` 实际为 **false**。**pure-stream 的触发阈值对真实消息等效排他**（仅 `es > max` 触发）。
  2. per-split 分支（L186-196）：`isRecordBetween(splitStart, splitEnd) && position.isAfter(split.getHighWatermark())` → 放行。`es ≤ 所属 HIGH` 的记录在此被丢弃（偏移有序读取下成立）。
  3. 否则 drop。
- `maxSplitHighWatermarkMap`（`configureFilter` L247-251）= 每表已 finished split 的 HIGH 的最大值。

### 4.4 水位来源（`queryCurrentOffset` MAX 语义）

`KafkaJsonKafkaOffsetUtils.queryCurrentOffset`：读每个分区最新一条消息（`endOffset-1`），从**消息内容**取 es/ts，返回**最大** `(maxEs, Integer.MAX_VALUE, Long.MAX_VALUE)`；topic 空 → `INITIAL_OFFSET`。TiDB+es 改用 `KafkaJsonTidbOffsetUtils.queryCurrentOffset`（TSO）。

---

## 5. 审计发现

### F1（证伪）——初稿判定的"精确边界 `es == max` 双发"不成立

**初稿机制**（已推翻）：HIGH 捕获时 `max = max(各分区最新消息 es)`，存在消息 `B.es == max`。初稿认为：回填发出 B 一次；流式 `offsetsForTimes(max)` 又把 B 读入；基座 pure-stream `position.isAtOrAfter((max,MAX,MAX))` 因 `es == max` 而**含端点放行** → B 双发。

**推翻证据**（OrderProbe，用真实 `KafkaJsonOffset` 类）：
```
boundary(3000,0,0).isAtOrAfter(HIGH(3000,MAX,MAX))  = false   // es == max 不触发 pure-stream
boundary(3000,0,0).isAfter(HIGH(3000,MAX,MAX))      = false   // 也不越过 per-split 上界
after(3001,0,0).isAtOrAfter(HIGH(3000,MAX,MAX))     = true    // 仅 es > max 触发
```
根因：水位哨兵 `(es, MAX, MAX)` 的 `partition=MAX` 保证任何真实消息（`partition < MAX`）在同 es 时**排在哨兵之前**。基座 `hasEnterPureStreamPhase` 的 `isAtOrAfter` 虽然比较层含端点，但对真实消息**等效排他**。B 在单 split 下：触发不了 pure-stream，per-split `isAfter(HIGH)` 也为 false → **被基座丢弃，不双发**。

**结论**：单 split、单分区（偏移有序读取）下，连接器的快照→增量切换**本就 exactly-once**，F1 的双发不发生，Tier 1 不是为修复"F1 双发"而生。**真正的缺陷是 F4（§5 F4）**——基座 per-split 过滤只在"已回填记录先于触发记录被读"时成立，一旦触发记录先被读到，过滤即被短路。

**为什么现有 ITCase 全绿（初稿解释仍对，但不再是唯一原因）**：4 个 Phase-11 ITCase 都刻意"先把增量消息写入 Kafka 缓冲、或快照完成后再写"——快照开始时 topic 为空 → `HIGH = INITIAL_OFFSET(es=-1)` → 回填窗口为空、流式走 startup-mode 兜底 seek。但这只是**没有覆盖** F4 路径，不是"绕开 F1 双发"——F1 双发本就存在与否需实证，现已证伪。

**与历史结论的关系**（ROADMAP 对照）：
- P0-1（排他上界 `[LOW, HIGH)`）：`es==max` 消息不回填，由流式 pure-stream 单独发出。该结论成立的前提是"流式含端点放行 `es==max`"，与实证（等效排他）矛盾——**P0-1 对 `es==max` 的归属判断也基于含端点假设**。实际代码（P0-4 含端点上界）下 `es==max` 只由回填发一次。
- P0-4（含端点上界 `[LOW, HIGH]`）：把 `(min, 真实边界]` 内全部记录交给回填（按主键与快照去重，**正确**）。P0-4 声称"流式发 `(HIGH, …)`"——在偏移有序读取下与实证**一致**（流式实际发 `es > max`），但需注意：这一"流式排他"靠的是基座 per-split `isAfter(HIGH)` 在**有序**读取下的行为，一旦 pure-stream 被触发（F4）即失效。

### F2（确认）——`KafkaJsonOffsetSupplier` 死代码

- `KafkaJsonSourceFetchTaskContext.getOffsetSupplier()`（L192-194）**无任何生产调用**（全仓 grep 仅定义处）。
- P0-4 把 `queryCurrentOffset` 改为 MAX 后只更新了该类的 javadoc；类体已退化为 `queryCurrentOffset` 的薄包装，且持有**从未被使用**的独立 `KafkaConsumer`（每构造一个就多一个连接）。建议删除或挂到 `displayCurrentOffset` 复用。

### F3（确认）——文档漂移

| 位置 | 现状 | 实际 |
|---|---|---|
| `EXACTLY_ONCE.md` §3 step 4 | 回填 `(LOW, HIGH]` | `[LOW, HIGH]`（下界含端点，P0-4 后） |
| `ROADMAP.md` P0-1"结论" | 排他上界 `[LOW, HIGH)` + `es==max` 由流式发 | 已被 P0-4 含端点上界取代；流式发 `es > max`（实证） |
| `ROADMAP.md` P0-4 | "流式发 `(HIGH, …)`" | 有序读取下成立；pure-stream 触发（F4）后失效 |

### F4（确认，真实缺陷，Tier 1+2 已闭合）——pure-stream 短路放行已回填记录

**机制**（代码级推导 + 影响面实证）：
1. 基座 `hasEnterPureStreamPhase`（L205-225）在**任一**记录 `position.isAtOrAfter(maxSplitHighWatermark)` 时把表加入 `pureStreamPhaseTables`，之后该表**所有**记录在 `shouldEmit` 中被直接放行（L206-207、L182-184 短路）。
2. **单分区（偏移有序）下不构成缺陷**：`es ≤ 所属 HIGH` 的已回填记录先于 `es > max` 的触发记录被读（同分区 offset 升序）→ 触发前已被 per-split `isAfter(HIGH)` 丢弃。
3. **多分区下构成缺陷**：连接器对 topic 全分区轮询，`poll()` 批次内跨分区记录顺序不保证（热分区新记录先到、冷分区旧记录后到）。若 `es > max` 的触发记录先被读到 → pure-stream 触发 → 之后读到的**已回填记录**（`es ≤ 所属 HIGH`、`es ≤ max`）被无条件放行 → **双发**。跨批次同理（批次 1 读触发记录、批次 2 读已回填记录）。
4. **多 split 放大了窗口**：`startingOffset = min(各 split HIGH)`，`es ∈ (min HIGH, 所属 HIGH]` 的记录严格在流式起点之后、但已在所属 split 的回填 `[LOW, 所属 HIGH]` 内发过一次。单分区下这些记录也在触发前被读、被 per-split 丢弃；多分区乱序下会在 pure-stream 触发后再次被放行。

**修复（已实现，§8）**：
- **Tier 1**：流式 split 入队前丢弃 `es ≤ startingOffset`。单 split 时 `startingOffset == 所属 HIGH` → 无论 pure-stream 是否已触发、无论跨分区乱序，`es ≤ HIGH` 的已回填记录都在入队前被丢 → **闭合单 split 的 F4 泄漏**。
- **Tier 2**：流式 split 入队前按 `finishedSnapshotSplitInfos` 预过滤，丢弃 `es ≤ 所属 split HIGH && isRecordBetween(所属范围)` 的记录 → **闭合多 split 的 F4 泄漏**（含 `es == 非最小 HIGH` 的边界残留与全部 `es ∈ (min HIGH, 所属 HIGH]`）。

> **审计修正（2026-08-14）**：初稿曾假设"多 split 并行时各 split 的回填窗口相互重叠 → 同一记录可能被多个 split 的回填发出（源级重复）"。逐行核对基座 `IncrementalSourceScanFetcher` 后排除：scan fetcher 对每条回填记录按 chunk key-range 过滤（`isChangeRecordInChunkRange`），同一记录只可能被**包含其 key** 的那一个 split 回填 → 源级不重复。初稿把 F4 记为"理论、需证伪"，现经机制推导确认为真实缺陷，且是**唯一**需要修复的边界缺陷。

### F5（确认）——TiDB+es（TSO）免疫

HIGH = TSO（DB 提交时钟），`es` 是提交时间戳；**不存在 `es == TSO` 的消息**（TSO 是查询时刻，消息已提交的 `es` 恒 `< TSO`）。流式 `offsetsForTimes(TSO)` 读到 `es < TSO` 的记录时：pure-stream 不触发（`es < TSO`）、per-split `isAfter(TSO)` 为 false → 全部过滤 → 不双发、不丢失。TSO 边界在语义上等价于 MySQL 的 binlog 字节位置——**落在消息之间**。这是四组合里唯一天然干净的一档，恰好也是本连接器 TiDB 链路默认配置（`databaseType=tidb, event-time=es`）。

---

## 6. 影响面评估

| `(databaseType, event-time)` | 水位来源 | 修复前 | 修复后（Tier 1+2） |
|---|---|---|---|
| MySQL + es（默认） | Kafka 采样 MAX | 单分区 exactly-once；多分区乱序下 F4 双发 | 单 split 由 Tier 1 闭合；多 split 由 Tier 2 闭合 |
| MySQL + ts | Kafka 采样 MAX | 同上 | 同上 |
| TiDB + ts | Kafka 采样 MAX | 同上 | 同上 |
| TiDB + es | TSO | **天然免疫** | 免疫 |
| snapshot-only | 同上 | 见 P0-1 已知边角 | 终点 `maxOffset` 有界 |

下游可见性：CREATE/UPDATE 在幂等 upsert 型 sink（StarRocks 等）不可见；INSERT 新行、DELETE、或非幂等/计数型消费端可见重复。**修复后多分区乱序下的重复被消除**（Tier 1 覆盖 `es ≤ min HIGH`，Tier 2 覆盖 `(min HIGH, 所属 HIGH]`；`es > 所属 HIGH` 的记录是 genuinely new，不重复）。

---

## 7. 验证建议（针对性回归）

1. **F4 单 split 回归（已做）**：`testStreamSplitDropsBoundaryMessageAtStartingOffset` —— 流式 split（无 finished infos）下 `es == startingOffset` 的边界消息被 Tier 1 丢弃、严格在其后的消息正常发出。对应单 split 的 F4 泄漏（即使基座 pure-stream 已触发，该消息也被入队前丢弃）。
2. **F4 多 split 回归（已做）**：`testStreamSplitDropsRecordsCoveredByEarlierSnapshotSplits` —— 两个 finished split（HIGH 3000 / 4000），断言 `es == 4000 == 所属 HIGH` 的边界记录被 Tier 2 丢弃，`es > 所属 HIGH` 的记录正常发出；同时验证 schema 中表缺省时的防御路径。
3. **F2 死代码清理**：删除 `KafkaJsonOffsetSupplier` + `getOffsetSupplier()`（或挂到 `displayCurrentOffset` 复用，删除独立 consumer）。
4. **F3 文档订正**：EXACTLY_ONCE §3 下界；ROADMAP P0-1 标注"已由 P0-4 取代"、P0-4 标注"流式排他依赖有序读取，pure-stream 触发（F4）后失效"。

---

## 8. 修复记录（2026-08-13 Tier 1；2026-08-14 Tier 2）

### F4 Tier 1（已实现）：流式 split 排他下界

**改动**：`KafkaJsonStreamFetchTask.processRecords`，在 `lastOffset` 计算之后、DDL/数据入队之前新增一条守卫：

```java
if (StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
        && lastOffset.isAtOrBefore(startingOffset)) {
    // ... 流式 split 严格读 > startingOffset；回填 split 不受影响
    continue;
}
```

- **触发面**：仅流式 split（`splitId == STREAM_SPLIT_ID`）；回填 split 的 `splitId` 是快照分片 id（基座 `createBackfillStreamSplit`），保持含端点 `[LOW, HIGH]`，必须继续发 `es == HIGH` 边界消息。
- **语义（修正后）**：这不是"让 pure-stream 含端点阈值失效"（§5 F1 证伪了该前提），而是**在入队前闭合 F4 的泄漏面**——`es ≤ startingOffset = min(各 split HIGH)` 的记录全部是已回填/已被快照覆盖的，无论基座 pure-stream 是否已触发、无论跨分区乱序顺序，它们都被丢弃而不是交给基座短路放行。
- **无损性**：`es ≤ startingOffset` 的记录，要么被回填 `[LOW, HIGH]`（含端点）经基座 scan fetcher 按键合并恰一次发出（`rewriteOutputBuffer`，已核实**只覆盖、不丢弃**），要么 `es < LOW` 效果已在 JDBC 快照里 → 流式丢弃不丢数据；`es > startingOffset` 全部保留。被丢弃的记录仍推进 consumer 位置与 `currentOffset`（checkpoint 进度不倒退）。
- **覆盖**：单 split（并行度 1）下 MySQL+es/ts、TiDB+ts 的 F4 泄漏完全闭合（`startingOffset == 所属 HIGH`）；TiDB+es（TSO）本就免疫，无副作用。

**测试**（`KafkaJsonStreamFetchTaskTest`）：
- 新增 `testStreamSplitDropsBoundaryMessageAtStartingOffset`：边界消息 `es == startingOffset (3000,MAX,MAX)` 被丢弃（`currentOffset` 仍推进到该消息、position 正常前移），严格在其后的 UPDATE 正常发出。
- 原四个"有界回填"单测改用新增的 `backfillSplit` 帮助方法（`splitId = "snapshot-split-0"`），保留含端点回填语义 —— 避免被流式排他下界误伤。
- `testConsumesConvertsAndTracksOffset` 的流式起始水位下移至首条消息之前（2999），消除边界歧义。

### F4 Tier 2（2026-08-14 已实现）：多 split 按 finished snapshot split 预过滤

多 split（并行度 > 1 或大表分 chunk）时 `startingOffset = min(各 split HIGH)`，`es ∈ (min HIGH, 所属 HIGH]` 的记录严格在流式起点之后，单靠 Tier 1 覆盖不到；一旦基座 pure-stream 被触发（F4），这些已被所属 split 回填的记录会被再次放行。修法：流式任务**入队前**按 `split.getFinishedSnapshotSplitInfos()` 逐条核对，丢弃"`es ≤ 所属 split HIGH` 且 `isRecordBetween(所属 split 范围)`"的记录：

```java
// KafkaJsonStreamFetchTask
private boolean isCoveredByFinishedSnapshotSplit(
        KafkaJsonSourceFetchTaskContext context,
        SourceRecord sourceRecord,
        KafkaJsonOffset lastOffset) {
    TableId tableId = context.getTableId(sourceRecord);
    if (context.getDatabaseSchema().tableFor(tableId) == null) {
        return false; // 表不在共享 schema，无法解析分片键类型：宁可不丢
    }
    for (FinishedSnapshotSplitInfo finishedSplit :
            finishedSplitsByTable.getOrDefault(tableId, Collections.emptyList())) {
        if (lastOffset.isAtOrBefore(finishedSplit.getHighWatermark())
                && context.isRecordBetween(
                        sourceRecord,
                        finishedSplit.getSplitStart(),
                        finishedSplit.getSplitEnd())) {
            return true; // 已由该 split 的有界回填发出过
        }
    }
    return false;
}
```

- **数据源**：与基座 `configureFilter`（L227-256）同源 —— `HybridSplitAssigner.createStreamSplit`（L228-276）把每个 finished snapshot split 的 `(tableId, splitStart, splitEnd, HIGH)` 装入 `StreamSplit.finishedSnapshotSplitInfos`；大表（分片数 > `splitMetaGroupSize`）经 `StreamSplitMetaEvent` 分片传输、`IncrementalSourceReader.fillMetaDataForStreamSplit` 合并后仍是完整列表（仅 `isCompletedSplit()` 后才交给 reader）。故流式任务总能拿到全量 finished split 信息。
- **判定**：`lastOffset.isAtOrBefore(HIGH)` 即 `es ≤ HIGH.es`（哨兵 `partition=MAX`，见 §4.1）；`isRecordBetween` 复用基座 `JdbcSourceFetchTaskContext`（L72-76）——按分片键从**转换后**记录取 key、`splitKeyRangeContains` 判范围。与基座 per-split `shouldEmit`（L186-196）使用同一套判定，只是把条件从"`isAfter(HIGH)` 放行"反转为"`isAtOrBefore(HIGH)` 丢弃"，并把判定从"纯-stream 触发后失效"提前到"入队前无条件成立"。
- **触发面**：仅流式 split；回填 split 不含 finished infos（`backfillSplit` 为空列表），天然不触发。
- **语义**：`es ≤ 所属 HIGH` 且属所属范围 → 已回填，丢弃；`es > 所属 HIGH` → genuinely new，放行。与单 split 下基座 per-split 分支在**有序读取**时给出的结果完全一致——Tier 2 把"有序读取才成立"的丢弃变成"无条件成立"，从而闭合多分区乱序下的 F4 泄漏。这也顺带覆盖了 F4 的"晚到 `es ≤ 阈值` 记录"分支（canal 跨分区乱序/发布滞后）。
- **无损性**：与基座"treated as covered"契约一致——`es ≤ 所属 HIGH` 的记录被认为已被该 split 的回填覆盖（回填窗口含端点）。极端情形：若该记录在回填**之后**才被 canal 写入 Kafka（发布滞后且落在窗口内），Tier 2 会丢弃它——这与单 split 单分区下基座 per-split 分支的既有行为完全一致，不是新引入的丢失，而是把既有"有序读取才成立"的语义推广到多分区。真正晚到且**越过**所属 HIGH 的记录（`es > 所属 HIGH`）不受影响，正常发出。

**测试**（`KafkaJsonStreamFetchTaskTest`）：
- 新增 `testStreamSplitDropsRecordsCoveredByEarlierSnapshotSplits`：构造两个 finished split（`[1,10)@HIGH 3000`、`[10,20)@HIGH 4000`），流式起始 `(3000,MAX,MAX)`；断言 `es == 4000` 且 pk=12（在 split-1 范围）的边界记录被 Tier 2 丢弃，`es == 4500`（pk=12）与 `es == 3200`（pk=5，在 split-0 范围但 > split-0 HIGH）正常发出；`currentOffset` 仍推进到最后一条、position 正常前移。

### 本次未做（独立清理项）

- **F2**：删除死代码 `KafkaJsonOffsetSupplier` + `getOffsetSupplier()`。
- **F3**：订正 `EXACTLY_ONCE.md` §3 下界 `[LOW, HIGH]`；`ROADMAP.md` P0-1 标注"已由 P0-4 取代"、P0-4 标注"流式排他依赖有序读取，pure-stream 触发（F4）后失效"。

---

## 9. 附录：关键代码引用

| 位置 | 内容 |
|---|---|
| `KafkaJsonStreamFetchTask.java` L236-250 | 流式 split 排他下界（Tier 1）：`STREAM_SPLIT_ID && lastOffset.isAtOrBefore(startingOffset)` → drop |
| `KafkaJsonStreamFetchTask.java` L252-270 | 回填窗口 `[LOW, HIGH]`：下界 `es < starting`、上界 `isAfter(ending)`（含端点） |
| `KafkaJsonStreamFetchTask.java` L287-299 | 流式 split 多 split 预过滤（Tier 2）：`isCoveredByFinishedSnapshotSplit(...)` → drop |
| `KafkaJsonStreamFetchTask.java` L330-349 | `isCoveredByFinishedSnapshotSplit`：`es ≤ 所属 HIGH && isRecordBetween(所属范围)` |
| `KafkaJsonStreamFetchTask.java` L368-413 | `assignAndSeek`：`offsetsForTimes(starting.es)` seek（按**记录时间戳**，非 es） |
| `KafkaJsonStreamFetchTask.java` L423-430 | `dispatchEndWatermark(endingOffset, END)` |
| `IncrementalSourceStreamFetcher.java` L178-203 | `shouldEmit`：pure-stream 短路放行（F4 根因） |
| `IncrementalSourceStreamFetcher.java` L205-225 | `hasEnterPureStreamPhase`：`isAtOrAfter(maxHigh)`；哨兵 `partition=MAX` 使其对真实消息等效排他 |
| `IncrementalSourceStreamFetcher.java` L227-256 | `configureFilter`：`maxSplitHighWatermarkMap` = 每表 max HIGH；`finishedSplitsInfo` 按表分组 |
| `JdbcSourceFetchTaskContext.java` L72-76 | `isRecordBetween`（Tier 2 复用） |
| `AbstractScanFetchTask.java` L48-116 | LOW→读→HIGH→(回填\|直派 END)；L118-126 建回填 split |
| `HybridSplitAssigner.java` L228-276 | 主 stream：起点 = min(各 split HIGH)；`finishedSnapshotSplitInfos` 装配（Tier 2 数据源） |
| `IncrementalSourceReader.java` L417-463 | `fillMetaDataForStreamSplit`：大表 finished infos 分片合并 |
| `KafkaJsonKafkaOffsetUtils.queryCurrentOffset` | MAX 语义 `(maxEs, MAX, MAX)`；空 topic → INITIAL |
| `KafkaJsonOffset` | 哨兵排序：消息 partition < MAX → 同 es 排在水位前 |
| `KafkaJsonSourceFetchTaskContext.getOffsetSupplier` L192-194 | 死代码（无生产调用） |
