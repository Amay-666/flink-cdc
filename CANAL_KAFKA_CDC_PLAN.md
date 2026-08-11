# Canal / Kafka 增量源 + JDBC 全量源 CDC Connector 规划文档

> 基线代码：Flink CDC `3.2.1`（分支 `feature/3.2.1-custom`）
> 参照实现：`flink-connector-postgres-cdc`（基于 `flink-cdc-base` 增量快照框架的新一代架构）
> 文档版本：v1.0（规划稿，尚未实现）

---

## 1. 需求与目标

### 1.1 业务需求

1. **增量同步**：外部工具（Canal / Debezium）把数据库的**增量数据变更 + DDL 变更**同步到 Kafka。本 connector 作为 **Kafka Source** 消费这些消息。
2. **全量同步**：首次同步时通过 **JDBC 分片（chunk）** 并行读取存量数据，与增量无缝衔接（全量切增量）。
3. **消息格式**：Kafka 消息为 **String**，支持 `canal`（flatMessage JSON）与 `debezium`（envelope JSON）两种格式，**优先实现 canal**。
4. **架构参照**：以 `flink-connector-postgres-cdc` 为蓝本，复用 `flink-cdc-base` 增量快照框架（FLIP-27 + Watermark Signal 算法）。
5. **交付物**：完整可编译、可测试的 connector 模块，含**全量→增量切换状态**、checkpoint 恢复、DDL 处理、新表捕获等核心能力，以及完整测试类。

### 1.2 设计范围与取舍

- **快照数据源**：canal 面向 MySQL，故快照 JDBC 源为 **MySQL**。方言层抽象，后续可扩展其他数据库。
- **增量数据源**：**Kafka**，topic 内消息为 canal flatMessage JSON（String）。
- **两种消费模式**（与 PG 对齐）：
  - **Pipeline 模式（YAML）**：输出 `org.apache.flink.cdc.common.event.Event`（CreateTableEvent / DataChangeEvent / SchemaChangeEvent）。
  - **SQL/Table API 模式**：输出 `RowData`（可选，Phase 9 实现）。
- **Exactly-once 语义**：借助 `flink-cdc-base` 的 watermark + backfill + `shouldEmit` 去重实现快照/增量合并；offset 仅存于 Flink 状态，Kafka group 不提交（可配置）。

### 1.3 关键结论（来自源码调研）

- 当前仓库**没有任何 Kafka 作为 CDC Source 的现成实现**，Kafka 仅作为 pipeline Sink（`flink-cdc-pipeline-connector-kafka`）。本模块需从零开发，但可复用：
  - `flink-cdc-base`：全部 split / assigner / reader / fetcher / event / watermark 机制。
  - `flink-connector-debezium`：`DebeziumEventDeserializationSchema`、`SourceRecordEventDeserializer`。
  - `flink-connector-mysql-cdc`：MySQL JDBC 连接、类型映射、表发现、DDL 解析素材（仅参考，不直接依赖旧框架类）。
- 框架内部统一以 **Kafka Connect `SourceRecord`** 为事件载体（snapshot 与 stream 都产出 SourceRecord，再由 deserializer 转 Event/RowData）。因此 **canal JSON → 合成 SourceRecord（debezium envelope 形状）** 是本设计的核心桥接层。

---

## 2. 架构总览

### 2.1 数据流

```
                 ┌────────────── 全量阶段（Snapshot Phase）──────────────┐
   MySQL ──JDBC──▶ CanalChunkSplitter ──▶ SnapshotSplit(chunk) ──▶ CanalScanFetchTask
                 │                            (多并行度)          │  watermark(LOW) → JDBC 读 chunk → watermark(HIGH) → backfill
                 └────────────────────────────────────────────────┘
                                                                         ▼
Canal ──Kafka──▶ CanalStreamFetchTask ◀── StreamSplit(minHighWatermark)  SourceRecord(Envelope)
外部工具写入       (KafkaConsumer 消费)         │                             │
                 │  canal flatMessage JSON ──▶ CanalRecordConverter ──▶ SourceRecord
                 └──────────────────────────────────────────────────────────────┘
                                                                         ▼
                                                        CanalEventDeserializer / CanalRecordEmitter
                                                                         ▼
                                        org.apache.flink.cdc.common.event.Event （Pipeline 模式）
                                        org.apache.flink.table.data.RowData（SQL 模式）
```

### 2.2 模块划分（新模块）

| 模块 | 路径 | 职责 |
|---|---|---|
| `flink-connector-canal-cdc` | `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-canal-cdc/` | 核心 source connector（SQL + 框架） |
| `flink-cdc-pipeline-connector-canal` | `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-canal/` | Pipeline 模式包装（DataSource/MetadataAccessor/EventDeserializer） |

> 注：`flink-cdc-pipeline-connector-mysql` 的 `MySqlDataSource`/`MySqlEventDeserializer` 是 pipeline 层的现成模板。若想减少模块数量，也可以把 DataSource 直接放进 `flink-connector-canal-cdc`（参考旧版 MySQL source connector 内置 pipeline 类的做法），Phase 10 决策。

### 2.3 依赖

```
flink-connector-canal-cdc
├── flink-cdc-base                     # 增量快照框架（必须）
├── flink-connector-debezium           # DebeziumDeserializationSchema / SourceRecordEventDeserializer
├── flink-connector-mysql-cdc          # (test) MySQL testcontainer + (可选 runtime) 复用 MySqlConnection/类型工具
├── mysql-connector-java               # JDBC 快照源（与 mysql-cdc 同版本 8.x）
├── kafka-clients                      # 原生 KafkaConsumer / KafkaProducer / AdminClient（偏移模型需要自控 consumer）
├── flink-connector-kafka              # 仅 test（KafkaContainer）与 SQL 端（可选）
├── jackson-databind                   # canal JSON 解析（仓库已统一版本）
├── com.alibaba:druid                  # DDL 解析器之一（scan.canalddl.parser=druid，默认）；需在根 BOM/dependencyManagement 增加版本管理
└── (test) org.testcontainers:mysql / org.testcontainers:kafka / flink-connector-test-util
```

> DDL 双解析器（§5.13）：Druid 实现为必选；Debezium ANTLR 实现依赖 `io.debezium:debezium-connector-mysql`（`MySqlAntlrDdlParser`，随 `flink-connector-mysql-cdc` 传递），标注 `optional` 依赖——仅当 `scan.canalddl.parser=debezium` 时才需要。

> 说明：`flink-connector-kafka` 3.0.2-1.18 的 `KafkaSource` 是 FLIP-27 源，无法直接塞进 base 框架的 split/fetch 模型（offset 无法进入 cdc 状态、无法按 timestamp 自定义 seek），因此**流式读取用原生 `kafka-clients` 的 `KafkaConsumer`**（同 PG 用原生 replication connection 的道理）。`flink-connector-kafka` 只承担 SQL 端可选能力与测试。

### 2.4 类清单（与 PG 一一对照）

| PG connector 类 | Canal connector 类 | 差异点 |
|---|---|---|
| `PostgresSourceBuilder` | `CanalSourceBuilder<T>` | 增加 kafka 选项 |
| `PostgresIncrementalSource` | `CanalSource<T>`（extends `JdbcIncrementalSource<T>`） | 同构 |
| `PostgresSourceOptions` | `CanalSourceOptions`（extends `JdbcSourceOptions`） | 增加 `properties.kafka.*` 等 |
| `PostgresSourceConfigFactory` | `CanalSourceConfigFactory` | 增加 kafka 参数 |
| `PostgresSourceConfig` | `CanalSourceConfig` | 持有 kafka 配置 + 格式 |
| `PostgresOffset` / `PostgresOffsetFactory` | `CanalOffset` / `CanalOffsetFactory` | 偏移从 LSN → **(eventTime, partition, offset)** |
| `PostgresDialect` | `CanalDialect` | `displayCurrentOffset()` 改为查 kafka 当前位点 |
| `PostgresChunkSplitter` / `PostgresQueryUtils` | `CanalChunkSplitter` / `CanalQueryUtils` | SQL 改为 MySQL 语法 |
| `PostgresTypeUtils` / `ChunkUtils` | `CanalTypeUtils` | MySQL 类型 → Flink 类型 |
| `CustomPostgresSchema` / `PostgresSchema` | `CanalSchema` | 轻量 `RelationalDatabaseSchema` |
| `PostgresScanFetchTask` | `CanalScanFetchTask` | 快照用 JDBC；backfill 改为读 kafka |
| `PostgresStreamFetchTask` | `CanalStreamFetchTask` | WAL 流 → KafkaConsumer 流 |
| `PostgresSourceFetchTaskContext` | `CanalSourceFetchTaskContext` | 持有 Kafka/AdminClient、dispatcher、schema |
| `PostgresSourceReader` | `CanalSourceReader` | offset 提交语义不同（kafka） |
| `PostgresSourceEnumerator` | `CanalSourceEnumerator` | 不再建 slot，改为校验 kafka 可连通性 |
| `PostgresSchemaChangeEventHandler` | `CanalSchemaChangeEventHandler` | 提取 canal DDL 的 source info |
| （PG 无） | `CanalRecordConverter` / `CanalRecordFactory` | **canal JSON ↔ SourceRecord 桥接（核心新增）** |
| `PostgreSQLTableFactory` / `PostgreSQLTableSource` | `CanalTableFactory` / `CanalTableSource` | SQL 入口 |
| `PostgreSQLReadableMetadata` | `CanalReadableMetadata` | 元数据列（database/table/op_ts 等） |
| `PostgreSQLDeserializationConverterFactory` | `CanalDeserializationConverterFactory` | SQL 模式 converter |
| `OffsetCommitEvent/AckEvent` | 复用 base，不新增 | — |

---

## 3. 核心数据模型

### 3.1 偏移模型 `CanalOffset`（全量→增量切换的基石）

框架要求 `Offset` 具备**全序**（`compareTo`）以支持 min/max 水位、`shouldEmit` 去重、checkpoint 持久化。PG 用 `Lsn`，Kafka 没有全局单调位点，因此：

```java
public class CanalOffset extends Offset {
    // getOffset() 序列化键：
    //   "eventTime"   → canal 消息时间戳（默认 es=binlog 执行时间；可配置 ts=canal 发送时间）
    //   "partition"   → kafka 分区
    //   "offset"      → kafka 分区内偏移
    // compareTo: (eventTime, partition, offset) 字典序
    public static final CanalOffset INITIAL_OFFSET = new CanalOffset(-1L, -1, -1L);
    public static final CanalOffset NO_STOPPING_OFFSET = new CanalOffset(Long.MAX_VALUE, -1, -1L);
}
public class CanalOffsetFactory extends OffsetFactory {
    newOffset(Map<String,String>) → CanalOffset
    createInitialOffset()          → CanalOffset.INITIAL_OFFSET
    createNoStoppingOffset()       → CanalOffset.NO_STOPPING_OFFSET
}
```

**为什么用 `(eventTime, partition, offset)` 而非纯 kafka offset**：

- 快照阶段的 LOW/HIGH watermark 必须能表达"此刻流的位置"，而多个 partition 没有单一 offset 可比 → 用**最大消息时间戳**作为全局位置（单调不减）。
- 流阶段 `shouldEmit` 判断"该消息是否已被快照覆盖"时，用 `position.isAfter(splitInfo.highWatermark)` → 时间戳可全序比较。
- `(partition, offset)` 作为时间戳相等时的 tie-breaker，保证同一 partition 内严格有序。

**一致性语义（文档中明确）**：

- 默认 `es`（binlog 执行时间）作为 eventTime，与 binlog 顺序一致，配合 canal 按 binlog 顺序写 kafka，快照/增量去重边界清晰。
- 需 canal 侧 `canal.mq.flatMessage=true` 且 `canal.instance.filter.query.dml=true` 等基础配置正常。
- 边界处（消息时间戳恰好等于某 split 高水位）存在**极窄的重复/丢失窗口**，默认行为是"时间戳 ≤ 高水位且主键在 split 范围内 → 丢弃"（严格一致快照优先）。若业务接受重复不接受丢失，可配置 `scan.canal.boundary.mode=at-least-once`（≤ 改为 <）。
- **多分区跨 partition 乱序**：若单 topic 多分区且 canal 未按主键 hash 分区，极小概率出现"后到但时间戳小"的消息被误丢弃。规划在文档/校验中提示：**canal 配置单分区或按主键 hash 分区以获得严格 exactly-once**。

### 3.2 事件载体 `SourceRecord`（canonical envelope）

框架的 `IncrementalSourceRecordEmitter`、`IncrementalSourceStreamFetcher.shouldEmit`、`JdbcSourceFetchTaskContext.isRecordBetween/rewriteOutputBuffer`、`DebeziumEventDeserializationSchema` 全部建立在 **debezium envelope 形状的 SourceRecord** 上。因此 canal 消息与 JDBC 快照行**统一转换为该形状**：

```java
// 数据变更 SourceRecord
key     : Struct{ id: INT32, ... }            // 主键列（供 isRecordBetween / getSplitKey）
value   : Struct{
    op        : "r"/"c"/"u"/"d",              // READ/INSERT/UPDATE/DELETE
    before    : Struct{...} | null,           // UPDATE/DELETE 的旧值（canal old）
    after     : Struct{...} | null,           // INSERT/UPDATE/快照行（canal data）
    source    : Struct{ db, schema, table, ts_ms, es, partition, offset },
    ts_ms     : long                          // fetch 时间
}
valueSchema: 由 CanalSchema 依据 MySQL 列类型构造的 typed connect Schema
sourceOffset: CanalOffset 的 Map
topic: 格式 "{serverName}.{db}.{table}"       // 供 getTableId 复用 SourceRecordUtils

// DDL 变更 SourceRecord
keySchema : "io.debezium.connector.canal.SchemaChangeKey"
value     : Struct{ historyRecord: "<HistoryRecord JSON>" }  // 含 database / ddl / tableChanges
```

> 关键点：快照行与流消息走**同一个 `CanalRecordFactory`**，保证形状一致、deserializer 统一处理。

### 3.3 Schema 模型

- `CanalSchema`：轻量 `RelationalDatabaseSchema` 实现，内部 `Map<TableId, Table>`（debezium relational `Table`），由 JDBC 元数据（`jdbc.readSchema`）懒加载构建。
- 用途：
  1. `CanalSourceFetchTaskContext.isRecordBetween` 取 split key 类型；
  2. `CanalChunkSplitter` 取 split 列；
  3. `CanalRecordFactory` 构造 typed `valueSchema`（canal 值全是 String，类型必须来自 DB schema，不能靠值推断）；
  4. `discoverDataCollectionSchemas` 产出 `TableChange`。
- `CanalTypeUtils`：MySQL `jdbc type / mysqlType` → `org.apache.flink.cdc.common.types.DataType` 与 Kafka Connect `Schema`（int/bigint/varchar/decimal/datetime/bit/json/…）。

---

## 4. 全量 → 增量切换机制（核心，复用 base + canal 特化）

### 4.1 状态机（复用，无需改动）

```
INITIAL_ASSIGNING --onFinish(全部snapshot完成+checkpoint)--> INITIAL_ASSIGNING_FINISHED
      ↑  (发现新表)                                                    │ startAssignNewlyTables
NEWLY_ADDED_ASSIGNING_FINISHED <--onStreamSplitUpdated-- NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED
      │                                                                  ▲
      └───────────────── startAssignNewlyTables ------------------------┘
```

由 `SnapshotSplitAssigner`（base）驱动；canal 侧**不重写状态机**，只提供正确的水位偏移。

### 4.2 单 chunk 快照流程（`AbstractScanFetchTask` 模板 + `CanalScanFetchTask`）

| 步骤 | 动作 | canal 特化 |
|---|---|---|
| 1 | `dialect.displayCurrentOffset()` → LOW 水位 | `CanalDialect` 查 kafka 当前最大消息时间戳（`CanalOffsetSupplier`，见 5.5） |
| 2 | `executeDataSnapshot()` | MySQL JDBC 读 chunk（`CanalQueryUtils.buildSplitScanQuery`），逐行 `CanalRecordFactory.build(READ)` 入队 |
| 3 | `displayCurrentOffset()` → HIGH 水位 | 同 1 |
| 4 | `backfill = createBackfillStreamSplit(low, high)` | `CanalScanFetchTask.executeBackfillTask`：起一个临时 KafkaConsumer，按 `(table, chunk 键范围)` 消费 `[low, high)` 时间窗消息，交给 base `IncrementalSourceScanFetcher.rewriteOutputBuffer` 合并（快照中被改动的行以流版本为准） |
| 5 | END 水位 / skipBackfill 时 HW=LW | 复用 |

### 4.3 全量完成 → 流切换（事件时序，全部复用 base）

```
Reader(各 chunk 完成后)
  ─ FinishedSnapshotSplitsReportEvent{splitId → highWatermark(CanalOffset)} ─▶ Enumerator
Enumerator
  ─ splitAssigner.onFinishedSplits(...) ─ FinishedSnapshotSplitsAckEvent ─▶ Reader
（所有 chunk 完成 + checkpoint 完成 → assignerStatus = INITIAL_ASSIGNING_FINISHED）
Enumerator.assignSplits()
  ─ HybridSplitAssigner.createStreamSplit()
      minOffset = min(所有 highWatermark)；maxOffset = max(...)
      FinishedSnapshotSplitInfo 列表（table, splitStart/End, highWatermark）
      若超 splitMetaGroupSize 先发空 meta 后续分批
  ─ StreamSplit 分配给 reader（优先 subtask-0）──▶ Reader
Reader
  ─ StreamSplitMetaRequestEvent / StreamSplitMetaEvent 分批取 meta
  ─ 提交 CanalStreamFetchTask（KafkaConsumer 起流）
      base IncrementalSourceStreamFetcher.shouldEmit 按 FinishedSnapshotSplitInfo 去重
```

### 4.4 流式读取与去重（`CanalStreamFetchTask` + base fetcher）

1. **seek**：`KafkaConsumer.offsetsForTimes(partition → startingOffset.eventTime)`，把每个 partition seek 到 ≥ 起始时间戳的第一个 offset；无匹配则 seek 到 latest。
2. **消费**：poll canal flatMessage JSON → `CanalRecordConverter` → 拆批后每条合成 SourceRecord 入队（`queue.enqueue(new DataChangeEvent(record))`）。
3. **去重**（base `IncrementalSourceStreamFetcher.shouldEmit`，无需改动）：
   - 表已进入纯流阶段（`position >= 该表所有 split 的 maxHighWatermark`）→ 直接发射；
   - 否则：消息主键在某个已结束 snapshot split 范围内 **且** `position > 该 split 高水位` → 发射；其余丢弃。
4. **DDL 消息**：`isDdl=true` → 构造 schema-change SourceRecord 入队（schema change 与信号事件恒被 base 发射）。

### 4.5 状态持久化与恢复（复用 base 序列化）

| 状态 | 载体 | 内容 | 序列化 |
|---|---|---|---|
| Enumerator 状态 | `HybridPendingSplitsState` | snapshot splits 剩余/已分配 + `isStreamSplitAssigned` | `PendingSplitsStateSerializer`（复用） |
| Reader 状态 | `SnapshotSplitState` / `StreamSplitState` | snapshot 的 highWatermark；stream 的 `startingOffset`（=已消费到的最新 CanalOffset） | `SourceSplitSerializer` v5（复用，`CanalOffsetFactory` 注入） |
| offset 外置提交 | `IncrementalSourceReaderWithCommit.lastCheckpointOffsets` | checkpointId → offset | 复用；可选 `notifyCheckpointComplete` 提交 kafka |

**恢复路径**：重启 → `CanalSource.restoreEnumerator(checkpoint)` → 恢复 `HybridPendingSplitsState`；reader 恢复 `StreamSplitState.startingOffset` → 流从该 offset 续读。快照未完成的 split 继续分配。**这套机制完全复用 base，canal 只需保证 `CanalOffset` 可正确序列化/比较**。

### 4.6 新表捕获（`scan.newly-added-table.enabled=true`）

复用 base 全部逻辑：`SnapshotSplitAssigner.captureNewlyAddedTables()` → `NEWLY_ADDED_ASSIGNING` → 新表 snapshot 完成 → 挂起 stream split（`toSuspendedStreamSplit`）→ `LatestFinishedSplitsNumberRequest/Event` → `toNormalStreamSplit` 恢复。canal 侧无需改动（新表的 DDL `CREATE TABLE` 消息也会产生 CreateTableEvent，配合快照）。

---

## 5. 组件详细设计

### 5.1 `CanalSourceOptions` / `CanalSourceConfigFactory` / `CanalSourceConfig`

继承 `JdbcSourceOptions` / `JdbcSourceConfigFactory` / `JdbcSourceConfig`。新增选项：

| 配置键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `properties.kafka.bootstrap.servers` | ✅ | — | kafka 地址（透传 `ConsumerConfig`） |
| `properties.kafka.group.id` | ❌ | 随机 | 消费组（默认不提交 offset） |
| `scan.kafka.topics` | ✅ | — | 逗号分隔 topic 列表（或正则） |
| `scan.message.format` | ❌ | `canal` | `canal` / `debezium` |
| `scan.canalddl.parser` | ❌ | `druid` | DDL 解析器：`druid`（Alibaba，默认）/ `debezium`（Debezium ANTLR） |
| `scan.canal.event-time` | ❌ | `es` | 偏移时间戳：`es`(binlog 时间) / `ts`(canal 时间) |
| `scan.canal.boundary.mode` | ❌ | `exactly-once` | 高水位等值边界处理 |
| `scan.kafka.startup.mode` | ❌ | `earliest` | 流 consumer 起始：`earliest`/`latest`/`timestamp`（快照启动时被水位机制覆盖，stream-only 时生效） |
| `scan.kafka.properties.*` | ❌ | — | 透传任意 kafka consumer 属性 |
| `scan.incremental.snapshot.chunk.size` 等 | ❌ | 8096 | 复用 `JdbcSourceOptions` |
| `scan.startup.mode` | ❌ | `initial` | `initial`/`snapshot`/`latest-offset`/`earliest-offset`/`timestamp`（复用 base） |

> `JdbcSourceConfig` 抽象方法 `getDbzConnectorConfig()` 需返回一个**轻量 `CommonConnectorConfig`**（提供 logicalName、schemaNameAdjuster、sourceInfoStructMaker 等，供 `JdbcSourceEventDispatcher`/水位事件构造使用）。PG 用 `PostgresConnectorConfig`，canal 侧自建最小实现（`CanalConnectorConfig`，约 10 个 getter）。

### 5.2 `CanalOffset` / `CanalOffsetFactory`（见 3.1）

### 5.3 `CanalDialect implements JdbcDataSourceDialect`

| 方法 | 实现 |
|---|---|
| `getName()` | `"canal"` |
| `discoverDataCollections(config)` | MySQL 元数据查询（`information_schema.tables` / `jdbc.readTableNames`），用 `databaseList/tableList` 过滤 |
| `discoverDataCollectionSchemas(config)` | `CanalSchema.getTableSchema(tableId)` → `TableChange` |
| `displayCurrentOffset(config)` | `CanalOffsetSupplier.current()`（见 5.5） |
| `isDataCollectionIdCaseSensitive(config)` | `false` |
| `createChunkSplitter(config)` | `new CanalChunkSplitter(config, this)` |
| `createFetchTask(split)` | snapshot → `CanalScanFetchTask`；stream → `CanalStreamFetchTask` |
| `createFetchTaskContext(config)` | `new CanalSourceFetchTaskContext(config, this)` |
| `openJdbcConnection(config)` | MySQL `JdbcConnection`（参考 mysql-cdc 的 `DebeziumUtils`） |
| `getPooledDataSourceFactory()` | MySQL 连接池工厂 |
| `queryTableSchema(jdbc, tableId)` | `jdbc.readSchema(...)` 或 `information_schema` |
| `notifyCheckpointComplete(id, offset)` | 可选：提交 kafka offset |
| `isIncludeDataCollection(config, tableId)` | 表过滤 |

### 5.4 `CanalChunkSplitter extends JdbcSourceChunkSplitter` + `CanalQueryUtils`

- `queryNextChunkMax`：`SELECT MAX(col) FROM (SELECT col FROM t WHERE col >= ? ORDER BY col LIMIT n) AS tmp`（MySQL LIMIT 语法）。
- `queryMinMax` / `queryMin`：对 split 列取 MIN/MAX，参数化下界。
- `queryApproximateRowCnt`：`SELECT table_rows FROM information_schema.tables WHERE ...`（近似值）。
- `fromDbzColumn`：委托 `CanalTypeUtils`。
- `buildSplitScanQuery` / `readTableSplitDataStatement`：chunk 范围查询（同 PG 模式，`splitStart/splitEnd` 边界）。

### 5.5 `CanalOffsetSupplier`（水位来源，快照阶段的关键）

- 打开一次 `AdminClient`（在 `CanalSourceFetchTaskContext` 初始化，reader 生命周期内复用）。
- `current()`：`listOffsets(LATEST)` → 每个 partition 最新 offset → `offsetsForTimes(partition→latestOffset)` 拿到最新消息时间戳 → 取**全局最大时间戳**，封装为 `CanalOffset`。
- 保证单调不减（kafka 只增长）。
- 若 topic 为空：返回 `INITIAL_OFFSET`。

> 备选：reader 内开一个**后台 KafkaConsumer**（类似 PG 复制槽），持续 poll 仅用于推进"当前位点"，`current()` 直接读其最后一条消息。首版用 AdminClient 方案（轻量、无状态、成本低）；若实测 `offsetsForTimes` 在大规模分区下有性能问题再切换。

### 5.6 `CanalRecordConverter` / `CanalRecordFactory`（核心新增，canal JSON ↔ SourceRecord）

`CanalRecordConverter`（流侧，**一个输入 → N 个输出**，因为 canal `data[]` 是批量）：

- 输入：`ConsumerRecord<String, String>` + 已解析的 `CanalFlatMessage`。
- 解析 canal flatMessage JSON（字段见附录 A）：
  - `isDdl=false`：`type` ∈ {INSERT, UPDATE, DELETE} → 遍历 `data[]` **逐条**调用 `CanalRecordFactory.build(op, tableId, beforeRow, afterRow, offset)`。一条消息产出 `data.length` 条 SourceRecord（同一条消息内所有记录共享同一 offset）。`beforeRow` 从 `old[]` 取（canal 保证 `old`/`data` 下标一一对应，UPDATE/DELETE 时）。
  - `isDdl=true`：`type` ∈ {CREATE, ALTER, DROP, TRUNCATE, RENAME, ...}，`sql` 为 DDL → 构造 1 条 **schema-change SourceRecord**（keySchema 匹配 `SCHEMA_CHANGE_EVENT_KEY_NAME`，value 内嵌 HistoryRecord JSON，含 `database` + `ddl`，**tableChanges 留空**——canal 不携带结构化列定义，由下游 deserializer 用配置的 DDL 解析器（`scan.canalddl.parser`，§5.13）解析 `sql` 补齐）。
- 表名 → `TableId`：`database + table`；若 `type` 含库级 DDL（无 table），按配置默认表处理或抛错/跳过并告警。
- 值转换：canal 值全为 String → 依据 `CanalSchema` 中该表的列类型，转成 typed 值后填入 `after/before Struct`（`CanalTypeUtils` 提供 String→Object 转换）。
- **key 构造**：canal 消息没有 key 数据，按 `pkNames` 从 `data`/`old` 行内提取主键列值组成 key Struct；无主键表（`pkNames` 空）退化为整行作 key（对齐 debezium 无主键行为）。

> **canal 为何必须"转成 debezium envelope 形状"（详见附录 C）**：base 框架的 `shouldEmit`/`isRecordBetween`/`rewriteOutputBuffer`/`DebeziumEventDeserializationSchema` 全部操作 SourceRecord 的 key/offset/struct 形状，且快照行与流消息必须共用同一 `CanalRecordFactory` 才能保证形状一致。这是**内部统一载体**，不是把输出改成 debezium——对外输出（Event/RowData）由 deserializer 决定。canal 与 debezium 格式**不能一一对应**，需补全：schema 靠 JDBC、key 靠提取、batch 靠拆分、DDL 靠可配置解析器（Druid / Debezium ANTLR，§5.13）。

`CanalRecordFactory`（快照 + 流共用）：

- `build(Operation op, TableId, Map<String,Object> before, Map<String,Object> after, CanalOffset offset)` → SourceRecord：
  - 构造 typed `valueSchema`（来自 `CanalSchema` 列定义）；
  - `key` Struct = 主键列；
  - `source` Struct = `{db, schema, table, ts_ms, es}`；
  - `sourceOffset` = offset 的 Map。
- 同时被 `CanalScanFetchTask`（JDBC 行 → `Map<String,Object>`）与 `CanalRecordConverter`（canal 消息）调用。

### 5.7 `CanalScanFetchTask extends AbstractScanFetchTask`

- `executeDataSnapshot(context)`：SQL 读 chunk（同 PG `PostgresSnapshotSplitReadTask`，但**不经过 debezium `SnapshotChangeRecordEmitter`**，直接用 `CanalRecordFactory.build(READ, ...)` 构造 SourceRecord，`queue.enqueue(new DataChangeEvent(record))`）——保证快照/流记录形状一致。
- `executeBackfillTask(context, backfillStreamSplit)`：临时 KafkaConsumer 消费 `[low, high)` 时间窗、仅取该 chunk 表 + 键范围的消息，转 SourceRecord 入队（base scan fetcher 会 `rewriteOutputBuffer` 合并）。
- 水位事件仍通过 `CanalSourceFetchTaskContext.getDispatcher().dispatchWatermarkEvent(...)`（复用 `JdbcSourceEventDispatcher`）。

### 5.8 `CanalStreamFetchTask implements FetchTask<SourceSplitBase>`

- 字段：`StreamSplit split`、`KafkaConsumer<byte[],byte[]> consumer`、`volatile boolean running`。
- `execute(context)`：
  1. `taskContext.getKafkaConsumer()` 获取（或新建）consumer；`subscribe(topics)`；按 `split.getStartingOffset().eventTime` 做 `offsetsForTimes` seek；
  2. 循环 `poll`：每个 `ConsumerRecord` → `CanalRecordConverter` → `List<SourceRecord>`（canal 批量消息拆分）逐条 `queue.enqueue`；处理 DDL 消息；
  3. 每秒发送 heartbeat/空转避免队列饥饿；
  4. 被 `close()` 时 `consumer.wakeup()` + `unsubscribe()` + 释放。
- `commitCurrentOffset(CanalOffset)`：可选，写入 `CanalStreamFetchTask` 持有的 offset 提交器。

### 5.9 `CanalSourceFetchTaskContext extends JdbcSourceFetchTaskContext`

必须实现（PG 同款抽象方法）：

| 方法 | 实现 |
|---|---|
| `getDatabaseSchema()` | `CanalSchema`（懒加载，含 `tableFor(tableId)` → Table） |
| `getSplitType(Table)` | `ChunkUtils.getSplitColumn(Table, chunkKeyColumn)` → RowType |
| `getDispatcher()` | 懒加载 `JdbcSourceEventDispatcher`（最小 `CanalConnectorConfig` + topicSelector + queue + filter + `CanalSchemaChangeEventHandler`） |
| `getOffsetContext()` | 最小 `OffsetContext` 实现（供 dispatcher 构造；snapshot 记录 offset 用 CANAL 占位） |
| `getPartition()` | 最小 `Partition`（`getSourcePartition()` 返回 `{serverName}`） |
| `getErrorHandler()` | `ErrorHandler`（记录异常 → 抛 FlinkRuntimeException） |
| `configure(split)` | snapshot：仅保留单表 filter；stream：配置 consumer seek 起点 |

新增能力：`getKafkaConsumer()`、`getCanalOffsetSupplier()`、`getSchema()`、`getQueue()`（与 dispatcher 共享同一 `ChangeEventQueue<DataChangeEvent>`）。

### 5.10 `CanalSourceReader extends IncrementalSourceReaderWithCommit`

- `snapshotState` / `notifyCheckpointComplete`：沿用基类，把 stream split 的 `startingOffset`（CanalOffset）写入 Flink 状态。
- 可选 kafka group 提交：`notifyCheckpointComplete` → `CanalStreamFetchTask.commitOffset`（默认关闭，offset 存状态即可 exactly-once）。
- 不需要 PG 的 `lsnCommitCheckpointsDelay` / `OffsetCommitEvent` 协调（除非后续做"新表 snapshot 期间暂停 offset 提交"，可仿 PG 加，规划默认不启用）。

### 5.11 `CanalSourceEnumerator extends IncrementalSourceEnumerator`

- 不需要建 slot；`start()` 里做一次 kafka 连通性/订阅 topic 存在性校验（`AdminClient.describeTopics`）。
- 其余继承 base。

### 5.12 反序列化（Pipeline 模式）

- `CanalEventDeserializer extends DebeziumEventDeserializationSchema`（参照 `MySqlEventDeserializer`）：
  - `isDataChangeRecord`：value schema 含 `op`；
  - `isSchemaChangeRecord`：keySchema name 匹配 `SchemaChangeKey`；
  - `getTableId`：从 source struct `db`/`table` 组成 2 段 `TableId`；
  - `getMetadata`：`{database, table, event_time, ...}`；
  - `deserializeDataChangeRecord`：复用基类（READ/CREATE→insert，DELETE→delete，UPDATE→update）；
  - `deserializeSchemaChangeRecord`：解析 HistoryRecord 中的 DDL → `CanalDdlParserFactory.create(config).parse(ddl, db)` → `List<SchemaChangeEvent>`（解析器由 `scan.canalddl.parser` 决定）。
- `CanalPipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event>`（参照 `MySqlPipelineRecordEmitter`）：
  - 快照 LOW 水位时按需发 `CreateTableEvent`（避免 checkpoint 超时，schema 从 `CanalSchema` 取）；
  - `initial` 模式 stream 起始时补发缓存 schema；`stream-only` 模式一次性发全部 CreateTableEvent。

### 5.13 DDL 解析：`CanalDdlParser` 接口 + 双实现（Druid / Debezium ANTLR）

**设计：接口抽象 + 可配置双实现**，输出统一为 cdc common `SchemaChangeEvent`（CreateTableEvent / AddColumnEvent / DropColumnEvent / RenameColumnEvent / AlterColumnTypeEvent / TruncateTableEvent 等）。由 `CanalDdlParserFactory` 依据 `scan.canalddl.parser` 选择（默认 `druid`）。

```java
public interface CanalDdlParser extends Serializable {
    /** 解析单条/多条 DDL 语句，输出变更事件；失败抛 SchemaOutOfSyncException */
    List<SchemaChangeEvent> parse(String database, String sql);
}
public class DruidCanalDdlParser implements CanalDdlParser { ... }
public class DebeziumAntlrCanalDdlParser implements CanalDdlParser { ... }
public class CanalDdlParserFactory {
    public static CanalDdlParser create(CanalSourceConfig config) { ... }
}
```

- **`DruidCanalDdlParser`（默认）**：`com.alibaba:druid` 的 `SQLUtils.parseSingleStatement(sql)` 解析为 AST（`MySqlCreateTableStatement` / `MySqlAlterTableStatement` / `MySqlDropTableStatement` / `MySqlTruncateTableStatement`），用 `MySqlSchemaStatVisitor` 提取表名/列/类型/主键/约束，映射到 `SchemaChangeEvent`。纯 Java、无代码生成，`statement.getDbType()` 判 MySQL 方言。
- **`DebeziumAntlrCanalDdlParser`（备选）**：依赖 `io.debezium:debezium-connector-mysql` 的 `MySqlAntlrDdlParser`（`optional` 依赖，仅此模式需要）；参照 pipeline-mysql 的 `CustomMySqlAntlrDdlParser` 写 `CustomCanalAntlrDdlParser` + 自定义 listener，输出 cdc `SchemaChangeEvent`。
- **共享层**：两种解析器都复用 flink-cdc 现成的 **MySQL 类型转换**（`MySqlSchemaConverter` / `ColumnConverter`：列类型字符串 → cdc `DataType`），差异仅在"SQL 文本 → 结构化变更（列/主键/约束）"这一层，避免重复实现。
- 兼容 canal 的 `type`（CREATE/ALTER/DROP/TRUNCATE/RENAME）。TRUNCATE 映射为 `TruncateTableEvent`（若存在）或 DELETE+提示。
- DDL 消息中 `sql` 解析失败时：记录 warn 日志 + 抛 `SchemaOutOfSyncException`（对齐 mysql-cdc 行为），由用户决定。
- **选择理由**：Druid 为阿里系（与 canal 生态同源）、API 直观、社区活跃，作为默认解析器；Debezium ANTLR 与 flink-cdc 主线其他 connector 一致，保留为对照/切换选项。

### 5.14 SQL / Table API 层（可选，Phase 9）

- `CanalTableFactory`：identifier = `canal-cdc`；required/optional options 同 PG 模式。
- `CanalTableSource implements ScanTableSource, SupportsReadingMetadata`：
  - `getScanRuntimeProvider` → `CanalSourceBuilder` 构建 `CanalSource<RowData>`，deserializer 用 `RowDataDebeziumDeserializeSchema` + `CanalDeserializationConverterFactory`；
  - metadata：`database_name` / `table_name` / `op_ts` / `event_time`。
- 注意：SQL 模式下 DDL 通常不发送给用户（changelog 约束），本层聚焦数据变更。

### 5.15 Pipeline 层（Phase 10）

- `CanalDataSource implements DataSource`：`getEventSourceProvider()` → `CanalSource<Event>` + `CanalPipelineRecordEmitter`；`getMetadataAccessor()` → `CanalMetadataAccessor`（基于 JDBC 元数据实现 `listNamespaces/listSchemas/listTables/getTableSchema`，参照 `MySqlMetadataAccessor`）。
- `CanalDataSourceFactory`：factory identifier = `canal`，解析 `properties.*`/`scan.*`。

---

## 6. 实施 Phase 规划（按依赖顺序，每个 phase 含测试）

> 约定：括号内为参照类；所有 phase 结束必须 `mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-canal-cdc -am compile` 通过。

### Phase 0：脚手架与注册
- 新建模块 `flink-connector-canal-cdc`（pom 复制 postgres 结构 + kafka-clients + jackson + mysql driver）；注册进 `flink-cdc-connect/flink-cdc-source-connectors/pom.xml`。
- `META-INF/services`：`CanalTableFactory` 占位（Phase 9 填充）。
- 空 source 包结构 + 骨架类 `CanalSource`/`CanalSourceBuilder`（仅构造，未接线）。
- 测试：无（或 `ModuleSmokeTest` 验证 classpath）。

### Phase 1：配置层
- `CanalSourceOptions` / `CanalSourceConfigFactory` / `CanalSourceConfig` / 最小 `CanalConnectorConfig`。
- 测试：`CanalSourceOptionsTest`（默认值/必填校验）、`CanalSourceConfigFactoryTest`（kafka props 透传、subtaskId 隔离）、`CanalConnectorConfigTest`。

### Phase 2：Offset 层
- `CanalOffset` / `CanalOffsetFactory`。
- 测试：`CanalOffsetTest`（compareTo 字典序、isBefore/isAfter、INITIAL/NO_STOPPING、serialize/deserialize 往返）、`CanalOffsetFactoryTest`、`CanalOffsetSerializerTest`（与 base `SourceSplitSerializer` 集成）。

### Phase 3：快照 JDBC 层
- `CanalQueryUtils` / `CanalChunkSplitter` / `CanalTypeUtils` / `CanalSchema` / `CanalDialect`（JDBC 部分）/ 连接池工厂。
- 测试：`CanalQueryUtilsTest`（SQL 生成、参数绑定）、`CanalChunkSplitterTest`（均匀/非均匀切分、单表多 chunk）、`CanalTypeUtilsTest`（MySQL 类型全覆盖映射）、`CanalSchemaTest`（JDBC 读 schema → Table/TableChange）。

### Phase 4：canal 消息桥接层（核心）
- `CanalFlatMessageParser` / `CanalRecordConverter` / `CanalRecordFactory` / 最小 `OffsetContext`/`Partition`。
- 测试：`CanalFlatMessageParserTest`（DML 三种 + DDL + 缺字段容错）、`CanalRecordConverterTest`（canal JSON → `List<SourceRecord>`：**多行 data 拆批**、key/value/source/sourceOffset/topic 断言）、`CanalRecordFactoryTest`（快照行与流消息产物一致性、typed 值转换、主键 Struct、无主键退化整行 key）。

### Phase 5：流式读取层
- `CanalStreamFetchTask` / `CanalOffsetSupplier`（AdminClient）。
- 测试：`CanalOffsetSupplierTest`（KafkaContainer 写数据后取最大时间戳）、`CanalStreamFetchTaskTest`（消费→入队→seek 起点、DDL 分流、结束/关闭）、`CanalStreamFetchTaskEndToEndTest`（KafkaContainer 起 topic、写 canal JSON，读回断言）。

### Phase 6：快照读取层 + Context
- `CanalSourceFetchTaskContext` / `CanalScanFetchTask` / `CanalSchemaChangeEventHandler`。
- 测试：`CanalSourceFetchTaskContextTest`（configure 单表过滤、dispatcher 构造）、`CanalScanFetchTaskTest`（Mock MySQL + Kafka 水位，断言 LOW→数据→HIGH→backfill→END 顺序）、`CanalSchemaChangeEventHandlerTest`。

### Phase 7：Reader / Enumerator 接线
- `CanalSourceReader` / `CanalSourceEnumerator` / `CanalSource.createReader/createEnumerator/restoreEnumerator`。
- 测试：`CanalSourceReaderTest`（split 状态推进、checkpoint snapshotState/restore）、`CanalSourceEnumeratorTest`（kafka 校验、split 分配）。

### Phase 8：反序列化 + DDL 解析（双解析器）
- `CanalEventDeserializer` / `CanalPipelineRecordEmitter` / `CanalDdlParser` 接口 + `CanalDdlParserFactory` / `DruidCanalDdlParser`（默认）/ `DebeziumAntlrCanalDdlParser`（备选）。
- 测试：
  - `DruidCanalDdlParserTest`（CREATE/ALTER ADD/DROP/RENAME/CHANGE COLUMN、DROP、TRUNCATE、多列类型、主键/索引）——逐 DDL 断言 `SchemaChangeEvent`；
  - `DebeziumAntlrCanalDdlParserTest`（同一组 DDL 样本）——断言输出与 Druid 一致；
  - `CanalDdlParserFactoryTest`（配置选型、非法值回退、两种模式实例化）；
  - `CanalEventDeserializerTest`（INSERT/UPDATE/DELETE → DataChangeEvent；DDL → SchemaChangeEvent；changelog 模式）；
  - `CanalPipelineRecordEmitterTest`（LOW 水位发 CreateTableEvent、initial/stream-only 缓存 schema）。

### Phase 9：SQL Table 层
- `CanalTableFactory` / `CanalTableSource` / `CanalReadableMetadata` / `CanalDeserializationConverterFactory`。
- 测试：`CanalTableFactoryTest`（identifier/required/optional options）、`CanalTableSourceTest`（metadata 列、runtime provider 类型）。

### Phase 10：Pipeline 层
- 新模块 `flink-cdc-pipeline-connector-canal`：`CanalDataSource` / `CanalDataSourceFactory` / `CanalMetadataAccessor`；注册 `META-INF/services`。
- 测试：`CanalDataSourceFactoryTest`（YAML 配置解析、identifier）、`CanalMetadataAccessorTest`（JDBC 元数据）。

### Phase 11：集成测试（重点，全量↔增量）
模块内 `src/test/java/.../source/` + `pipeline/source/`：

| 测试类 | 场景 |
|---|---|
| `CanalSourceInitialITCase` | initial：MySQL 有存量 + canal JSON 已入 kafka → 全量+增量合并，行断言 |
| `CanalSourceSnapshotOnlyITCase` | snapshot-only：仅全量（有界） |
| `CanalSourceStreamOnlyITCase` | latest/earliest：仅增量 |
| `CanalSourceFullToIncrementalITCase` | **核心**：快照进行中写入增量，验证 watermark/backfill/shouldEmit 切换后无重无漏 |
| `CanalSourceRestartITCase` | 中途 checkpoint → 恢复 → 续读（offset 从状态恢复） |
| `CanalSourceDdlITCase` | canal DDL 消息（ALTER ADD/DROP/RENAME）→ SchemaChangeEvent，schema 演进后数据正确 |
| `CanalSourceNewlyAddedTableITCase` | `scan.newly-added-table.enabled=true`：运行中新表 CREATE + snapshot |
| `CanalSourceMultiTableITCase` | 多库多表、复合主键、无主键表（全量告警/配置） |
| `CanalSourceTypeITCase` | MySQL 全类型（int/bigint/decimal/datetime/json/bit/enum/set/…）canal 值还原 |
| `CanalSourceSkipBackfillITCase` | `scan.incremental.snapshot.backfill.skip=true` 的 at-least-once 行为 |
| `CanalPipelineITCase` | pipeline 模式端到端（Event → 校验 sink） |
| `CanalTableFactoryITCase` | SQL `CREATE TABLE ... WITH('connector'='canal-cdc')` 端到端 |

测试基础设施（新增 `testutils/`）：
- `KafkaTestResource`：`KafkaContainer`（复用 pipeline-kafka 测试的 `KafkaUtil` 模式）+ topic 创建 + 一个 **`CanalJsonTestProducer`**（用 `KafkaProducer` 直接写手工构造的 canal flatMessage JSON，含 DDL/INSERT/UPDATE/DELETE）。
- `MySqlTestResource`：`MySqlContainer` + schema/数据初始化 SQL。
- `CanalJsonMessageBuilder`：构造各类 canal 消息的测试工具。
- 断言工具复用 `flink-connector-test-util` 的 `AssertUtils` / `TestSourceContext`。

### Phase 12：文档与收尾
- `docs/content.zh/docs/connectors/...` connector 使用文档（配置项、canal 侧要求、一致性说明）。
- 全量 ITCase 在 CI 常驻（`@Tag`/profile 对齐现有 connector）。

---

## 7. 测试策略与质量门禁

- **单测**：每个 Phase 配套；`CanalOffset` 与 `CanalRecordConverter` 为最高优先（其余逻辑依赖它们）。
- **ITCase**：KafkaContainer + MySQLContainer，网络受限环境跳过（沿用现有 connector 的 CI profile）。
- **回归**：所有测试基于真实 canal flatMessage 样本（附录 A 固化 3 条 DML + 3 条 DDL JSON 为测试资源文件 `src/test/resources/canal/`）。
- 质量门禁：`mvn -pl flink-connector-canal-cdc -am verify` 全绿后才进入下一 Phase。

---

## 8. 风险与待决策

| # | 风险/决策 | 影响 | 缓解/建议 |
|---|---|---|---|
| 1 | **多 partition 跨分区乱序**导致的极端一致性窗口 | 数据重复/丢失（窄窗口） | canal 配单分区或按主键 hash；文档明示；提供 boundary.mode 配置 |
| 2 | **快照慢于 kafka retention** | 丢失早期增量 | 校验 `scan.startup.mode=earliest` + 提示 retention ≥ 全量时长；可用 `latest` 先启动流再补全量 |
| 3 | **canal 值全 String 的类型还原** | 类型错误 | 类型一律以 `CanalSchema`（JDBC）为准，`mysqlType` 兜底；Phase 4/11 全类型 ITCase |
| 4 | **DDL 解析复杂度** | SchemaChangeEvent 不完整 | 双解析器（Druid 默认 + Debezium ANTLR 对照），共享类型转换层；Phase 8 双实现等价性测试；失败即 `SchemaOutOfSyncException` |
| 5 | **`CanalConnectorConfig`（最小 CommonConnectorConfig）工作量** | dispatcher 构造受阻 | 参照 `MySqlConnectorConfig` 精简；仅实现 dispatcher/水位用到的 getter |
| 6 | **是否先做 Pipeline 还是 SQL** | 工作量排序 | 建议 Pipeline（Event）优先，SQL 为 P1 追加；两者共享 bridge 层 |
| 7 | **模块名/标识符** | 对外契约 | 规划用 `flink-connector-canal-cdc` / SQL `canal-cdc` / pipeline `canal`；可改 |
| 8 | **是否复用 mysql-cdc 的 JDBC 连接层** | 依赖方向 | 建议**拷贝/适配**而非反向依赖 pipeline-connector-mysql；mysql-cdc source connector 的 `MySqlConnection`/`MySqlTypeUtils` 仅参考 |

---

## 附录 A：canal flatMessage 样本（测试资源基准）

**INSERT**
```json
{"id":1,"database":"test","table":"users","pkNames":["id"],"isDdl":false,"type":"INSERT",
 "es":1598752886000,"ts":1598754586044,"sql":"",
 "sqlType":{"id":4,"name":12},"mysqlType":{"id":"int(11)","name":"varchar(255)"},
 "data":[{"id":"1","name":"Alice"}],"old":null}
```

**UPDATE**
```json
{"id":2,"database":"test","table":"users","pkNames":["id"],"isDdl":false,"type":"UPDATE",
 "es":1598752887000,"ts":1598754586055,"sql":"",
 "sqlType":{"id":4,"name":12},"mysqlType":{"id":"int(11)","name":"varchar(255)"},
 "data":[{"id":"1","name":"Bob"}],"old":[{"name":"Alice"}]}
```

**DELETE**
```json
{"id":3,"database":"test","table":"users","pkNames":["id"],"isDdl":false,"type":"DELETE",
 "es":1598752888000,"ts":1598754586066,"sql":"",
 "sqlType":{"id":4,"name":12},"mysqlType":{"id":"int(11)","name":"varchar(255)"},
 "data":null,"old":[{"id":"1","name":"Bob"}]}
```

**DDL / CREATE**
```json
{"id":4,"database":"test","table":"orders","pkNames":null,"isDdl":true,"type":"CREATE",
 "es":1598752889000,"ts":1598754586077,"sql":"create table orders(id int not null auto_increment primary key, amount decimal(10,2), created_at datetime)",
 "sqlType":null,"mysqlType":null,"data":null,"old":null}
```

**DDL / ALTER**（`sql` 含完整语句，`table` 可空）
```json
{"id":5,"database":"test","table":"orders","isDdl":true,"type":"ALTER",
 "es":1598752890000,"ts":1598754586088,"sql":"alter table orders add column note varchar(255) null",
 "data":null,"old":null}
```

> canal 版本差异：`type` 枚举可能含 `QUERY`/`TRUNCATE`/`RENAME`/`ERASE`；`isDdl` 恒为 `true` 的是 DDL。`data`/`old` 可能为 `null`。消息 key 可为空（连接器不需要 key，主键从 `data`/`old` 取）。

---

## 附录 B：与 PG connector 的复用对照（最终结论）

- **全部复用（零改动）**：`JdbcIncrementalSource`、`IncrementalSourceEnumerator`、`IncrementalSourceReaderWithCommit`、`IncrementalSourceRecordEmitter`、`IncrementalSourceSplitReader`、`IncrementalSourceScanFetcher`、`IncrementalSourceStreamFetcher`、`HybridSplitAssigner`/`SnapshotSplitAssigner`/`StreamSplitAssigner`、`AssignerStatus`、全部 `SourceEvent`/`Split`/`State`/`Serializer`、`PendingSplitsStateSerializer`、`JdbcSourceChunkSplitter`、`JdbcSourceEventDispatcher`、`AbstractScanFetchTask`、`SourceRecordUtils`。
- **必须新建（约 25 个类 + 测试）**：见 §2.4 右列与 §5。
- **最大工作量**：Phase 4（canal JSON↔SourceRecord 桥接）> Phase 8（DDL 解析）> Phase 11（集成测试）。

---

## 附录 C：canal ↔ debezium 格式映射（能否一一对应？）

**结论：不能一一对应，必须外部补全/适配。** 唯一天然对应的是 DML 的 before/after/op；其余维度均存在结构差异（见下表）。因此本设计的核心桥接层 `CanalRecordConverter` 承担"补全"职责。

| 维度 | canal flatMessage | debezium envelope (SourceRecord) | 对应关系 | 桥接处理 |
|---|---|---|---|---|
| 单条 vs 批量 | 一个消息含 `data[]` 数组（可多行） | 一条 record = 一行 | ❌ 非一一对应 | **拆批**：按 `data` 下标逐条构造，同消息共享 offset |
| 类型信息 | 仅 `sqlType`/`mysqlType` 元信息，值全 String | 自带完整 typed connect `Schema`（每列类型/可选性） | ❌ schema 不在消息里 | 用外部 `CanalSchema`（JDBC 元数据）+ `CanalTypeUtils` 补全 typed schema 与值转换 |
| 主键 key | 仅 `pkNames` 列名，无 key 值 | key 独立 Struct（主键值） | ⚠️ | 按 `pkNames` 从 `data`/`old` 行提取建 key；无主键退化为整行 key |
| before/after | `old` / `data`（String） | `before` / `after`（typed） | ✅ 下标一一对应 | 值转 typed 后直接填入 |
| op | INSERT / UPDATE / DELETE（+DDL type） | c / u / d / r / t | ✅ | INSERT→c、UPDATE→u、DELETE→d、快照行→r；TRUNCATE→t（下游不支持则转 TruncateTableEvent/提示） |
| 时间戳 | `es`(binlog 时间) / `ts`(发送时间) | `source.ts_ms` / `ts_ms` | ✅ | `es`→`source.ts_ms`（作为偏移 eventTime 的基准） |
| binlog 元信息 | 无 file/pos/gtid | `file`/`pos`/`gtid`/`server_id`/`query` | ❌ 缺失 | 桥接时省略——offset 模型只用时间戳，不需要 binlog 位置 |
| DDL | `isDdl=true` + `type` + `sql` 纯文本 | keySchema=`SchemaChangeKey`，value 内嵌 HistoryRecord（含结构化 `tableChanges`） | ⚠️ canal 无结构化列定义 | 构造 SchemaChangeKey record，`tableChanges` 留空；下游 deserializer 用配置的解析器（`scan.canalddl.parser`：Druid 默认 / Debezium ANTLR 备选）解析 `sql` 生成 `SchemaChangeEvent` |
| 输出事件 | — | — | — | **统一载体 SourceRecord ≠ 对外输出**：最终产物是 `Event`（pipeline）/ `RowData`（SQL），由 deserializer 决定 |

**为什么统一到 SourceRecord（debezium envelope 形状）**：
1. base 框架机制全部构建在 SourceRecord 之上——`shouldEmit` 去重、`isRecordBetween`、`rewriteOutputBuffer`、`formatMessageTimestamp`、`IncrementalSourceRecordEmitter` 的类型分发、`DebeziumEventDeserializationSchema` 的 before/after 提取。
2. 快照（JDBC 行）与流（canal 消息）必须共用同一个 `CanalRecordFactory` 产出**形状一致**的 SourceRecord，这是全量↔增量切换去重正确性的前提。
3. 未来支持 debezium 格式（需求 1.1 的后续项）时，debezium JSON 天然吻合 SourceRecord，转换成本趋近于零。

**代价与边界（已接受）**：schema 依赖 JDBC 查询（需数据库可达）；DDL 依赖可配置解析器解析 sql 文本（Druid 默认 / Debezium ANTLR 备选，§5.13，双实现等价性测试保证行为一致）；无主键表与 TRUNCATE 走退化路径；`source` 结构不完整（file/pos 缺省）——均不影响本 connector 的偏移与去重逻辑。
