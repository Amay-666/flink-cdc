# jdbc-kafka-json-cdc 连接器：架构、数据流与扩展指南（含 RENAME 事件 Plan A）

> 本文档供后续自行扩展开发使用。覆盖：模块与依赖关系、数据流步骤、事件模型与序列化栈、
> 状态归属、关键注意点（坑）、扩展模板。
> 配套文档：[EXACTLY_ONCE.md](./EXACTLY_ONCE.md)（全量→增量切换机制）。
>
> 分支：`feature/canal-rename-plan-a`（基于 `feature/3.2.1-custom`，flink-cdc 3.2.1）。

---

## 命名与格式扩展（改名背景）

> 本连接器最初叫 **canal**，但初衷是解析 canal / debezium 等工具写入 Kafka 的 **JSON 消息**，名字过于片面。
> 整体改名为 **`jdbc-kafka-json-cdc`**，分层命名：

| 层 | 命名 | 说明 |
|---|---|---|
| **用户可见** | 模块 `flink-connector-jdbc-kafka-json-cdc` / `flink-cdc-pipeline-connector-jdbc-kafka-json`；SQL/pipeline factory identifier = `jdbc-kafka-json-cdc`；配置键 `scan.message.format`、`scan.database.type`、`scan.ddl.parser`、`scan.message.event-time`、`scan.boundary.mode` | 一眼看出"JDBC 快照 + Kafka 上的 JSON 消息" |
| **开发者** | package `org.apache.flink.cdc.connectors.kafkajson`；类前缀 `KafkaJson*` | 与用户可见名解耦，短好写 |
| **wire 格式** | `scan.message.format` 的值 `canal` / `debezium`（`MessageFormat` 枚举）；`scan.database.type` 的值 `mysql` / `postgres`（`DatabaseType` 枚举） | 引用外部工具的真实格式，**不随改名变化** |

> **扩展口子（只写了配置，没写实现）**：`scan.database.type` 选 JDBC/dialect 层，`scan.message.format` 选
> 消息解析层。当前版本只实现 **canal 格式 + MySQL**；其他组合（如 debezium 消息、PG 快照）在
> `KafkaJsonSourceConfigFactory.create()` 里 **fail-fast**（`IllegalArgumentException`），不会跑到深层报错。
> 后续加 PG：把 `KafkaJsonSourceConfigFactory` 的 JDBC driver/port 默认/dialect 按 `DatabaseType` 分支即可；
> 加 debezium 格式：新增 `MessageFormat.DEBEZIUM` 分支的消息解析路径（对应 `KafkaJsonFlatMessageParser`）。

---

## 0. 部署模型（先记住这个前提）

本连接器的使用方式是 **只打包 pipeline 模块 jar 作为 Flink source**，下游是**自定义 DataStream 算子**：

```java
FlinkSourceProvider provider =
        (FlinkSourceProvider) new KafkaJsonDataSource(factory).getEventSourceProvider();
env.fromSource(provider.getSource(), WatermarkStrategy.noWatermarks(), "canal-source")
        .process(new KafkaJsonRenameStateOperator())   // 你的下游算子
        .sinkTo(yourSink);
```

**没有 SchemaOperator、没有 YAML pipeline。** 后果：连接器只负责把 canal 消息变成 `Event` 流，
**不替你维护任何下游表状态**；下游算子必须自己从 `CreateTableEvent` / `RenameTableEvent` 维护和迁移状态。
这是整个文档反复出现的主题。

---

## 1. 模块与依赖总览

### 1.1 两个模块 + released 边界

| 模块 | 路径 | 职责 | 是否可改 |
|---|---|---|---|
| **source 模块** | `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-jdbc-kafka-json-cdc` | 原始 FLIP-27 `KafkaJsonSource<SourceRecord>`：消费 Kafka、canal 消息桥接、快照 JDBC、**DDL 双解析器**、`KafkaJsonSchemaChangeHandler` | ✅ 本项目可改 |
| **pipeline 模块** | `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-jdbc-kafka-json` | `KafkaJsonDataSource`（flink-cdc `DataSource`）+ **Event 层**：`KafkaJsonEventDeserializer`、`RenameTableEvent`、自定义序列化栈、参考下游算子 | ✅ 本项目可改（shade source 模块 + debezium + kafka，产出可部署 jar） |
| **released 模块** | `flink-cdc-common` / `flink-cdc-runtime` / 其他 connector | 发行版本 | ❌ **绝不可改**（硬约束） |

**Plan A 的核心策略**：新事件 `RenameTableEvent` 和它需要的序列化逻辑**全部复制/新建到 pipeline 模块内**，
released 的 common/runtime **零改动**。

### 1.2 类依赖关系

```
【数据源侧 source 模块】
KafkaJsonSourceBuilder<T> ──new──> KafkaJsonSource<T>
                                 ├─ createReader ──> KafkaJsonSourceReader (base IncrementalSourceReader)
                                 │                     └─ new KafkaJsonStreamFetchTask / KafkaJsonScanFetchTask (base 工厂回调)
                                 ├─ createEnumerator (base IncrementalSourceEnumerator)
                                 └─ createSourceSplitSerializer
KafkaJsonStreamFetchTask ──uses──> KafkaJsonFlatMessageParser（parse flatMessage JSON）
                              ├─ KafkaJsonRecordConverter（flatMessage → List<SourceRecord>）
                              └─ KafkaJsonSchemaChangeHandler（DDL 分支）
                                    ├─ KafkaJsonDdlParser（接口）→ KafkaJsonDruidDdlParser（默认）| KafkaJsonDebeziumDdlParser（ANTLR）
                                    ├─ KafkaJsonDdlParsedResult（before/after 双像） + KafkaJsonTableChangeType（枚举）
                                    ├─ KafkaJsonSchema（源侧状态，registerTable/removeTable/tableFor）
                                    └─ enqueueSchemaChange → schema-change SourceRecord 入队
KafkaJsonScanFetchTask ──uses──> KafkaJsonSnapshotSplitReadTask（JDBC 快照）+ KafkaJsonSchema

【Event 侧 pipeline 模块】
KafkaJsonDataSource ──getEventSourceProvider──> FlinkSourceProvider.of(KafkaJsonEventSource)
KafkaJsonEventSource extends KafkaJsonSource<Event>   ← 复用上排全部 fetch task，仅换 emitter
      └─ KafkaJsonPipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event>
             ├─ 惰性 CreateTableEvent（LOW watermark / stream-split 开始）
             └─ 普通记录交给 → KafkaJsonEventDeserializer
KafkaJsonEventDeserializer extends DebeziumEventDeserializationSchema
      ├─ getProducedType() → KafkaJsonEventTypeInfo        ← 序列化栈接缝
      ├─ 数据记录 → DataChangeEvent（tableId 来自 source 结构 db/table）
      ├─ schema-change 记录 → convertTableChange / handleRenameTable → Create/Add/Drop/AlterType/RenameColumn/RenameTableEvent
      └─ 内部瞬态 Tables 注册表（仅 ALTER diff 用）
KafkaJsonSchemaUtils / KafkaJsonTypeUtils（pipeline 版，映射到 common DataType）
event/RenameTableEvent implements SchemaChangeEvent
serializer/KafkaJsonEventTypeInfo → KafkaJsonEventSerializer → KafkaJsonSchemaChangeEventSerializer → KafkaJsonRenameTableEventSerializer
example/KafkaJsonRenameStateOperator（ProcessFunction<Event,Event>，参考下游）
```

**扩展时的关键接缝**：
- 换序列化栈：`KafkaJsonEventDeserializer.getProducedType()`（唯一入口）。
- 换下游处理：`.process(...)` 里的算子（完全在你的 job 里）。
- 新事件/新 DDL：见 §8。

---

## 2. 总体数据流（两阶段）

以默认 `scan.startup.mode=initial` 为例（全量快照 → 增量切换）：

```
                     ┌─────────────────────── INITIAL 启动 ───────────────────────┐
                     │                                                            │
   MySQL ──全量──> KafkaJsonScanFetchTask ──JDBC 分片──> KafkaJsonSnapshotSplitReadTask    │
                     │ 逐行 → KafkaJsonRecordConverter → SourceRecord                  │
                     │                │                                            │
                     │                ▼                                            │
                     │      ChangeEventQueue（共享队列）                           │
                     │                │                                            │
Kafka(canal 写入) ──增量──> KafkaJsonStreamFetchTask ──poll──> KafkaJsonFlatMessageParser  │
                     │      │                                                      │
                     │      ├─ 数据消息 → KafkaJsonRecordConverter → SourceRecord ──────┤
                     │      │                                                      │
                     │      └─ DDL 消息 → KafkaJsonSchemaChangeHandler                  │
                     │            └─ 改源侧 KafkaJsonSchema + 产出 schema-change       │
                     │               SourceRecord ─────────────────────────────────┤
                     │                                                             │
                     ▼                                                             │
      KafkaJsonSourceReader（base IncrementalSource）                                  │
                     │  split 切换、watermark 信号算法、emit                      │
                     ▼                                                             │
      KafkaJsonPipelineRecordEmitter ──> KafkaJsonEventDeserializer                        │
                     │  ├─ LOW watermark：惰性 CreateTableEvent（已发表去重）      │
                     │  ├─ 数据记录 → DataChangeEvent                              │
                     │  └─ schema-change → Create/Add/Drop/AlterType/RenameCol/RenameTable
                     ▼                                                             │
      下游算子（KafkaJsonRenameStateOperator 等）—— 自己维护按 TableId 的状态           │
                     ▼                                                             │
      Sink                                                                         │
                     └─────────────────────────────────────────────────────────────┘
```

快照阶段与增量阶段产出的 SourceRecord **格式一致**（`KafkaJsonSchema`/`KafkaJsonRecordFactory`/`KafkaJsonRecordConverter`
共享同一个 `KafkaJsonRecordFactory` 实例），所以下游看到的 Event 流是统一的。

---

## 3. 增量阶段主链路（数据 / DDL 分叉）

`KafkaJsonStreamFetchTask.processRecords`（逐条 poll 批次）：

```
for each ConsumerRecord:
    KafkaJsonFlatMessage message = KafkaJsonFlatMessageParser.parse(record.value())
    offset = new KafkaJsonOffset(eventTime(message), partition, offset)
    if message.isDdl():
        ──> KafkaJsonSchemaChangeHandler.handle(context, message, offset)     // 无数据记录
    else:
        sourceRecords = KafkaJsonRecordConverter.convert(message, ...)
        for each sourceRecord: queue.enqueue(new DataChangeEvent(sourceRecord))
```

### 3.1 数据消息路径

```
KafkaJsonFlatMessage（JSON：data/old/type/database/table/es/ts/isDdl/sql/columns...）
  → KafkaJsonRecordConverter.convert
  → Debezium 形状的 SourceRecord（envelope：before/after/source/op/ts_ms）
  → KafkaJsonEventDeserializer.isDataChangeRecord（op 字段非空）
  → DataChangeEvent
  → 下游
```

### 3.2 DDL 消息路径

```
KafkaJsonFlatMessage(isDdl=true, sql, database, table)
  → KafkaJsonSchemaChangeHandler.handle
       ├─ ddlParser.parse(db, tableId, currentTable, sql)     // currentTable = KafkaJsonSchema.tableFor
       ├─ 返回 null → 跳过（不改变 schema 的 DDL）
       ├─ applySchemaChange → 改源侧 KafkaJsonSchema（见 §6 L1）
       └─ if isIncludeSchemaChanges:  enqueueSchemaChange
              └─ 构造 schema-change SourceRecord（keySchema.name = io.debezium.connector.canal.SchemaChangeKey）
                 → 入队
  → KafkaJsonEventDeserializer.isSchemaChangeRecord（按 keySchema.name 判断）
  → convertTableChange（CREATE/ALTER/DROP 表变化数组）或 handleRenameTable（见 §4）
  → SchemaChangeEvent 子类
  → 下游
```

> **为什么 DDL 不走 base 的 `JdbcSourceEventDispatcher`？** Debezium 1.9.8 把
> `CommonConnectorConfig.isSchemaChangesHistoryEnabled()` 硬编码为 `false`，base 的 dispatcher 永远不会
> 入队 schema-change 记录。所以 `KafkaJsonSchemaChangeHandler` **绕过 dispatcher 手搭** schema-change
> SourceRecord（格式与 base 完全一致），`KafkaJsonPipelineRecordEmitter`/`KafkaJsonEventDeserializer` 原样消费。

---

## 4. RENAME_TABLE 全链路（Plan A 核心新增）

整条链路上每类改动的文件名都标出，方便对照代码。

```
MySQL:  RENAME TABLE `users` TO `vip_users`
  │ canal 把这条 DDL 写进 Kafka（flatMessage.isDdl=true）
  ▼
KafkaJsonStreamFetchTask.handleDdlMessage
  ▼
KafkaJsonSchemaChangeHandler.handle                        [source/handler/KafkaJsonSchemaChangeHandler.java]
  │ ddlParser.parse(...)
  ▼
KafkaJsonDruidDdlParser.parse                              [source/ddl/KafkaJsonDruidDdlParser.java]
  │ 命中 MySqlRenameTableStatement / SQLAlterTableRename → parseRenameTable
  │ （Debezium 版：KafkaJsonDebeziumDdlParser 在 parsed==null 时用 findRenamedTable 检测）
  ▼
KafkaJsonDdlParsedResult.renameTable(oldId, newId, oldTable, newTable)   type = RENAME_TABLE
                                                  [source/ddl/KafkaJsonDdlParsedResult.java]
  ▼
KafkaJsonSchemaChangeHandler.applySchemaChange            ── RENAME_TABLE 分支 ──
  │   KafkaJsonSchema.removeTable(oldTableId)                              【L1 状态改】
  │   KafkaJsonSchema.registerTable(newTable)  （newTable 非空时）
  ▼
KafkaJsonSchemaChangeHandler.enqueueSchemaChange
  │   tableChanges.create(newTable)                         // schema 以单个 CREATE change 携带
  │   historyDocument.set(canalTableChangeType, "RENAME_TABLE")
  │   historyDocument.set(canalNewTableId, "test.vip_users") // dbz TableId 字符串
  │   → schema-change SourceRecord 入队
  ▼
KafkaJsonEventDeserializer.deserializeSchemaChangeRecord     [pipeline source/KafkaJsonEventDeserializer.java]
  │ isRenameTableChange?(historyRecord)  → handleRenameTable
  │   oldTableId  ← record.source.db/table                  // 旧名
  │   newTableId  ← io.debezium.relational.TableId.parse(canalNewTableId)
  │                     → KafkaJsonSchemaUtils.toCommonTableId
  │   newTable    ← findCreatedTable(historyRecord)         // 取单个 CREATE change
  │   tables.removeTable(old) + tables.overwriteTable(new)  【L2 状态改】
  │   sql         ← historyRecord 的 DDL_STATEMENTS
  │   schema      ← KafkaJsonSchemaUtils.toSchema(newTable)（null 时空 schema）
  ▼
RenameTableEvent(oldTableId, newTableId, schema, sql)      [pipeline event/RenameTableEvent.java]
  ▼
下游算子：instanceof RenameTableEvent → 迁移自己的状态 old→new
        [pipeline example/KafkaJsonRenameStateOperator.java]    【L3 状态改，必须由下游做】
```

**自定义字段**（`KafkaJsonSchemaChangeHandler` 常量，被 deserializer 读取）：

| 字段 | 值 | 含义 |
|---|---|---|
| `canalTableChangeType` | `"RENAME_TABLE"` / `"RENAME_COLUMN"` | 标记这是 Debezium TableChangeType 表达不了的 rename |
| `canalNewTableId` | `"db.table"` 字符串 | RENAME_TABLE 新表 id（dbz TableId） |

**列改名（RENAME_COLUMN）**：`KafkaJsonDruidDdlParser` 认 `SQLAlterTableRenameColumn`；
pipeline 侧 `KafkaJsonEventDeserializer.diffTable` 还有**同位置同类型启发式**兜底（旧列消失 + 新列同名位置出现
→ `RenameColumnEvent`，common 原生支持，零公共 API 改动）。

---

## 5. 事件模型与序列化栈

### 5.1 事件类型树

```
org.apache.flink.cdc.common.event.Event
├─ FlushEvent                                    EventClass.FLUSH_EVENT
├─ DataChangeEvent                               EventClass.DATA_CHANGE_EVENT
└─ SchemaChangeEvent                             EventClass.SCHEME_CHANGE_EVENT
     ├─ CreateTableEvent     KafkaJsonSchemaChangeTag.CREATE_TABLE
     ├─ AddColumnEvent       ADD_COLUMN
     ├─ DropColumnEvent      DROP_COLUMN
     ├─ AlterColumnTypeEvent ALTER_COLUMN_TYPE
     ├─ RenameColumnEvent    RENAME_COLUMN
     └─ RenameTableEvent     RENAME_TABLE   ← 本项目新增（pipeline event/）
```

### 5.2 序列化栈（自包含，不碰 released）

```
KafkaJsonEventDeserializer.getProducedType() → new KafkaJsonEventTypeInfo()     ← 唯一接缝
  └─ createSerializer() → KafkaJsonEventSerializer.INSTANCE                 [serializer/KafkaJsonEventSerializer.java]
        （released EventSerializer 的本地副本；SchemaChangeEvent 委托给 KafkaJsonSchemaChangeEventSerializer）
        └─ KafkaJsonSchemaChangeEventSerializer.INSTANCE                     [serializer/KafkaJsonSchemaChangeEventSerializer.java]
             instanceof 分派：
             ├─ 5 个 released per-event serializer（字节格式与 released 一致）
             └─ RENAME_TABLE → KafkaJsonRenameTableEventSerializer.INSTANCE   [serializer/KafkaJsonRenameTableEventSerializer.java]
```

### 5.3 字节格式与"关闭分派"（必读）

- **released 的序列化器按 CLASS `instanceof` 分派，不按 `getType()` 枚举值分派。**
  所以 released `SchemaChangeEventSerializer` 收到一个它不认识的类就会 `else throw`。
- 自定义 `KafkaJsonSchemaChangeEventSerializer` 用**自己的 tag 枚举 `KafkaJsonSchemaChangeTag`**（6 值，含
  `RENAME_TABLE`），仍是 `instanceof` 分派 → 自定义类走自定义 tag。**新旧序列化器可以互读 5 种已知事件**
  （同一个 byte 格式），只有 RENAME_TABLE 是新增 tag。
- `RenameTableEvent.getType()` 返回 `SchemaChangeEventType.CREATE_TABLE`（**占位值**）——released 枚举里没有
  RENAME_TABLE。占位值只影响"按 getType 走 generic 逻辑"的代码；自定义序列化栈按 class 分派不受影响。
  **边界：别把 RenameTableEvent 喂给 released 的 `SchemaManager`/`SchemaDerivation`/`EventSerializer`**，
  它们不认这个类（会 throw 或误判为 CREATE_TABLE）。本部署模型（自建序列化栈）不会经过它们。

---

## 6. 状态归属（三层）与数据处理顺序

### 6.1 三层状态，各管各的

| 层 | 类 | 状态 | 谁改 | 用途 |
|---|---|---|---|---|
| **L1 源侧** | `KafkaJsonSchemaChangeHandler.applySchemaChange` → `KafkaJsonSchema` | source 侧"当前 schema 记忆" | 每条 DDL | 快照读表（KafkaJsonScanFetchTask:78）、`tableFor` 给 ALTER parser |
| **L2 反序列化侧** | `KafkaJsonEventDeserializer` 的瞬态 `Tables` | 注册表 | CREATE/RENAME/DROP 时 | **仅** ALTER 列级 diff（old vs new） |
| **L3 下游** | **你的算子**（如 `KafkaJsonRenameStateOperator`） | 按 TableId 的业务状态 | 收到 CreateTableEvent/RenameTableEvent | 最终消费状态 |

**关键结论**：
1. L1/L2 都是连接器内部工作寄存器，**不会传给下游**。
2. **数据事件的 tableId 不来自任何注册表**，来自 SourceRecord `source` 结构的 `db`/`table`
   （`KafkaJsonEventDeserializer.getTableId`）。`RENAME TABLE` 之后 binlog/canal 消息的 table 就是新名，
   所以**后续 DataChangeEvent 天然带新名**，不需要状态参与。
3. 下游的"新表加入"（CREATE）和"改名迁移"（RENAME）**必须由下游自己处理**——连接器只发事件。
   这就是 `KafkaJsonRenameStateOperator` 存在的意义（`perTableState.remove(old).put(new)`）。

### 6.2 事件发射顺序（下游会看到的顺序）

- **INITIAL 启动**：快照阶段每个 split 的 **LOW watermark** 时，`KafkaJsonPipelineRecordEmitter` **惰性**发
  该 split 表的 `CreateTableEvent`（`alreadySendCreateTableTables` 去重，避免 checkpoint 超时）；
  快照→增量切换（stream split 开始）时，把缓存中**未发过**的 `CreateTableEvent` 一次性补发
  （用缓存 schema 而非重新查库，避免比排队中的 schema-change 事件新）。
- **纯流式（非 initial）**：stream split 开始把全部 `CreateTableEvent` 一次发完。
- **流中 DDL**：按 Kafka 顺序进入队列，`KafkaJsonEventDeserializer` 按序转成
  `CreateTableEvent`/`AddColumnEvent`/`DropColumnEvent`/`AlterColumnTypeEvent`/`RenameColumnEvent`/`RenameTableEvent`。
- **数据事件**：紧跟其后，tableId 已是最新。

> 注意：`CreateTableEvent` 有两个来源——(a) 惰性/缓存补发（从 JDBC schema 取），(b) 流中 DDL CREATE
> （从 history record 取）。**同表 id 只会发一次**（已发表集合 + DDL 只在实际 CREATE 时产生）。下游按
> 幂等处理更稳。

### 6.3 已知限制（ALTER 对"流中未见 CREATE"的表）

L2 注册表**只被流中的 CREATE schema-change 记录填充**（快照阶段惰性发的 CreateTableEvent 不经过
`convertTableChange`）。所以：**job 启动前就存在的表，之后来了 ALTER，L2 里没有旧表 → diff 为空 →
不发列级事件**（deserializer `convertTableChange` ALTER 分支 `oldTable==null` → skip）。这是有意的
保守行为（避免对已存在表发 CreateTableEvent 被下游 SchemaManager 拒绝）。扩展时若需要这类表的 ALTER
列级事件，需要在快照阶段就把表 schema 灌进 L2。

---

## 7. 关键注意点清单（坑）

### 构建与测试
1. **构建必须用 `-am test`**：
   `mvn -q -o -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-jdbc-kafka-json -am test -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true`
   —— pipeline 模块依赖 `flink-connector-jdbc-kafka-json-cdc:test-jar`，本地 m2 没有 canal artifacts，
   `compile`/`install` 都失败（缺 jar-plugin 传递依赖），只有 `-am` 在 reactor 里构建并处理 test-jar。
2. **Checkstyle ImportOrder**（别加 `-Dcheckstyle.skip=true`，会漏掉 import 顺序错误）：
   分组 `org.apache.flink, org.apache.flink.shaded, *, javax, java, scala`，组间空行。
   **`*` 组把 io.debezium + org.apache.kafka + org.junit + org.assertj 等合并成一个字母序块，块内无空行**；
   但 `org.apache.flink` 组与 `*` 组之间**必须有空行**（新文件最容易漏这个）。
3. **JDK17 + Flink 测试 harness 冲突**：`ProcessFunctionTestHarnesses`/`OneInputStreamOperatorTestHarness`
   在 JDK17 触发 Chill/Kryo 反射 `Arrays$ArrayList` 的 `InaccessibleObjectException`（root pom surefire
   没有 `--add-opens`）。**参考算子测试直接驱动 `open()` + `processElement()`**（该算子 open 不需要
   RuntimeContext、processElement 不用 Context），不碰 harness。

### 类型与命名
4. **`Schema` 全限定**：`KafkaJsonEventDeserializer` 里已 import kafka 的 `org.apache.kafka.connect.data.Schema`，
   所以 common 的 schema 必须写全限定 `org.apache.flink.cdc.common.schema.Schema`。
5. **`KafkaJsonEventTypeInfo` 必须重写 `equals`/`hashCode`**（`instanceof KafkaJsonEventTypeInfo` +
   `getClass().hashCode()`）。released `EventTypeInfo` 的 equals 是 `instanceof` 而 hashCode 按类名，
   子类不改会违反 equals/hashCode 契约。
6. **`TableChanges.drop(Table)` 收的是 Table 不是 TableId**——DROP 记录要 `Table.editor().tableId(id).create()`。
7. **`VARCHAR(0)` 抛异常**：测试里建 VARCHAR 列 `length` 必须 > 0。
8. **`Document.toJson()` 不存在**：用 `DocumentWriter.defaultWriter().write(document)` 生成 historyRecord 字符串。
9. **`OperationType` 是顶层类** `org.apache.flink.cdc.common.event.OperationType`（不是嵌套）。
10. **`KafkaJsonDdlParsedResult` 没有 `getTable()` alias**——用 `getNewTable()`（历史遗留，别写错）。

### 设计约束
11. **released serializer 是"关闭分派"**（`instanceof` + `else throw`）：新事件必须复制一份序列化器到本地
    （pipeline serializer 包），用**自己的 tag 枚举**，不能往 released 加 case。
12. **`RenameTableEvent.getType()` 占位值 CREATE_TABLE**：只影响 generic 代码；自定义序列化栈按 class 分派
    不受影响。**别把它喂给 released SchemaManager/SchemaDerivation/EventSerializer**。
13. **`include.schema.changes` 默认 false**：关闭时 DDL 只改源侧 KafkaJsonSchema，**不发 schema-change 记录**
    （下游看不到 CreateTableEvent/RenameTableEvent）。你的 job 里开起来才能感知 rename。
14. **数据事件 tableId 来自 source 结构**，不来自 L1/L2 注册表——验证/单测时 source 的 db/table 字段要写对。
15. **下游状态不自动迁移**：RenameTableEvent 只是通知，迁移必须由下游算子做。
16. **`KafkaJsonSchema` 快照用途**：`KafkaJsonScanFetchTask` 把 `getDatabaseSchema()` 传给 `KafkaJsonSnapshotSplitReadTask`
    （快照读表用），别在 DDL handler 之外乱动它。

---

## 8. 扩展指南（模板）

### 8.1 新增一种 SchemaChangeEvent（如显式 DROP_TABLE 事件）

要动的文件（全在 pipeline 模块，released 零改动）：
1. `event/XxxEvent.java`——复制 `RenameTableEvent` 模板（`implements SchemaChangeEvent`，字段 + equals/hashCode/toString）。
2. `serializer/KafkaJsonXxxEventSerializer.java`——复制 `KafkaJsonRenameTableEventSerializer` 模板。
3. `serializer/KafkaJsonSchemaChangeEventSerializer.java`——`KafkaJsonSchemaChangeTag` 加一个值；`copy`/`serialize`/`deserialize`
   三个方法各加一个 `instanceof`/`case` 分支。
4. 产出点：`KafkaJsonEventDeserializer` 里对应分支 `new XxxEvent(...)`。
5. 测试：`KafkaJsonEventSerializerTest` 加 round-trip；`KafkaJsonEventDeserializerTest` 加产出来源。

### 8.2 新增 DDL 识别（如 `ALTER TABLE ... PARTITION BY`）

1. `source/ddl/KafkaJsonDruidDdlParser.java`——加 Druid AST 类型分支；`KafkaJsonDebeziumDdlParser.java`——加 ANTLR 分支。
2. `source/ddl/KafkaJsonTableChangeType.java`——加枚举值；`KafkaJsonDdlParsedResult.java`——加工厂方法。
3. `source/handler/KafkaJsonSchemaChangeHandler.java`——`applySchemaChange` 和 `enqueueSchemaChange` 加 type 分支
   （以及需要的话新 custom history-record 字段）。
4. `KafkaJsonEventDeserializer`——加对应事件产出。

### 8.3 修改数据事件字段（DataChangeEvent 结构）

`DataChangeEventSerializer` 是 released 的（不能改）。做法：复制到 pipeline `serializer/` 包成为
`KafkaJsonDataChangeEventSerializer`，在 `KafkaJsonEventSerializer` 里替换引用。**同一个 job 内新旧序列化器字节
不兼容**，改前想清楚是否需要兼容旧 checkpoint。

### 8.4 常见改动点速查

| 想做什么 | 改哪 |
|---|---|
| 换事件序列化栈 | `KafkaJsonEventDeserializer.getProducedType()` |
| 改 DDL 识别 | `source/ddl/*Parser` + `KafkaJsonDdlParsedResult` + `KafkaJsonSchemaChangeHandler` |
| 改源侧状态 | `KafkaJsonSchema`（registerTable/removeTable） |
| 改 ALTER 列级 diff | `KafkaJsonEventDeserializer.diffTable` |
| 下游感知 rename | 你的算子 + `KafkaJsonRenameStateOperator` 参考 |
| 快照阶段灌 L2 注册表 | `KafkaJsonPipelineRecordEmitter` 或 `KafkaJsonEventDeserializer` 构造点 |

---

## 9. 构建与测试命令

```bash
# source 模块
mvn -q -o -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-jdbc-kafka-json-cdc test \
  -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true

# pipeline 模块（含依赖，必须 -am）
mvn -q -o -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true

# 只跑单个测试类
... test -Dtest='KafkaJsonEventDeserializerTest,KafkaJsonEventSerializerTest' -DfailIfNoTests=false ...
```

当前测试基线（Plan A 完成后）：source 模块 140 绿，pipeline 模块 20 绿（deserializer 10 + serializer 2 +
factory 7 + reference operator 1），Reactor BUILD SUCCESS。

---

## 10. 已知边界与未来工作

- **RenameTableEvent 占位 getType**：若将来要接 released 的 SchemaOperator/SchemaManager（换回 YAML pipeline），
  需要把 `RenameTableEvent` + `SchemaChangeEventType.RENAME_TABLE` 做到 common/runtime（那就违反"不改 released"
  约束，属于 Plan B）。
- **流中未见 CREATE 的 ALTER 不产列级事件**（§6.3）——扩展点。
- **下游 keyed 状态迁移**：参考算子用 plain map 演示；真实 job 用 Flink `MapState` 时，RenameTableEvent 处理
  要做 keyed 上下文的 copy+delete（在 `KeyedProcessFunction` 里按 old/new tableId 两把 key 操作）。
- **Phase 11 集成测试**（全量↔增量，testcontainers MySQL+Kafka）尚未执行（本地无 Docker）。
- **列改名 RENAME_COLUMN 已全链路落地**（parser + diff → common 原生 `RenameColumnEvent`）。
