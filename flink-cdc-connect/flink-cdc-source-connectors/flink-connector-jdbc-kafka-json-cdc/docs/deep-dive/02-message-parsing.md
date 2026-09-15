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

# 消息解析层：canal / Debezium 桥接与 DDL 解析

> 本文讲连接器的**消息入口**：Kafka 上的 canal flatMessage JSON（以及 Debezium 格式）如何被解析、拆批、
> 转成统一的 SourceRecord；DDL 如何被双解析器解析成 `SchemaChangeEvent`。
> 关联：[03-event-model.md](./03-event-model.md)（事件模型）、[01-exactly-once.md](./01-exactly-once.md)（水位机制）。

---

## 1. 为什么要有"桥接层"

外部工具（canal / Debezium）把数据库变更写成 **JSON 消息**写进 Kafka。但 flink-cdc-base 框架内部统一以
**Kafka Connect `SourceRecord`** 为事件载体——`shouldEmit` 去重、`isRecordBetween`、`rewriteOutputBuffer`、
`DebeziumEventDeserializationSchema` 全部操作 SourceRecord 的 key/offset/struct 形状。

因此 **canal / Debezium JSON → 合成 SourceRecord（debezium envelope 形状）** 是核心桥接层。快照（JDBC 行）
与流（Kafka 消息）共用同一个 `KafkaJsonRecordFactory`，产出**形状一致**的 SourceRecord，这是全量↔增量
切换去重正确性的前提。

> **canal ↔ debezium 格式不能一一对应**：唯一天然对应的是 DML 的 before/after/op；其余维度（单条 vs 批量、
> 类型信息、主键 key、binlog 元信息、DDL 结构）都有差异，必须由桥接层补全。

---

## 2. 消息格式与配置开关

| 配置键 | 值 | 说明 |
|---|---|---|
| `scan.message.format` | `canal`（默认）/ `debezium` | 唯一格式开关，解析器由它决定 |
| `scan.database.type` | `mysql` / `postgres` / `tidb` | 选 JDBC/dialect 层；`tidb` 复用 MySQL 兼容路径 |
| `scan.ddl.parser` | `druid`（默认）/ `debezium` | DDL 解析器选择 |

当前实现 **canal 格式 + MySQL/TiDB**；其他组合（如 PG 快照）在 `KafkaJsonSourceConfigFactory.create()`
里 **fail-fast**（`IllegalArgumentException`），不会跑到深层报错。

---

## 3. canal flatMessage 桥接

### 3.1 消息形状

canal flatMessage（`canal.mq.flatMessage=true`）：

```json
{"id":1,"database":"test","table":"users","pkNames":["id"],"isDdl":false,"type":"INSERT",
 "es":1598752886000,"ts":1598754586044,"sql":"",
 "data":[{"id":"1","name":"Alice"}],"old":null}
```

关键点：
- **一个消息含 `data[]` 数组**（可多行）——需**拆批**：按 `data` 下标逐条构造 SourceRecord，同消息共享 offset。
- 值**全 String**，类型必须来自外部 DB schema（`KafkaJsonSchema` / JDBC 元数据），不能靠值推断。
- 主键从 `data`/`old` 行内提取组成 key Struct；无主键表（`pkNames` 空）退化为整行作 key。
- `es`/`ts` 是事件时间来源（见 [01-exactly-once.md](./01-exactly-once.md)）。

### 3.2 数据消息路径

```
CanalMessage（JSON）
  → KafkaJsonRecordConverter.convert
  → Debezium 形状的 SourceRecord（envelope：before/after/source/op/ts_ms）
  → KafkaJsonEventDeserializer.isDataChangeRecord（op 字段非空）
  → DataChangeEvent
```

### 3.3 批量与 key

- **拆批**：`data.length` 条 → 逐条 `KafkaJsonRecordFactory.build(op, tableId, beforeRow, afterRow, offset)`。
  `beforeRow` 从 `old[]` 取（canal 保证 `old`/`data` 下标一一对应，UPDATE/DELETE 时）。
- **key 构造**：canal 消息没有 key 数据，按 `pkNames` 从行内提取主键列值组成 key Struct。

---

## 4. DDL 消息路径与双解析器

### 4.1 DDL 消息路径

canal 的 DDL 消息 `isDdl=true`，`sql` 为 DDL 文本。连接器**绕过 base 的 `JdbcSourceEventDispatcher`**（Debezium
1.9.8 把 `isSchemaChangesHistoryEnabled()` 硬编码为 false，base dispatcher 永远不会入队 schema-change 记录），
由 `KafkaJsonSchemaChangeHandler` 手搭 schema-change SourceRecord（格式与 base 完全一致）：

```
CanalMessage(isDdl=true, sql, database, table)
  → KafkaJsonSchemaChangeHandler.handle
       ├─ ddlParser.parse(db, tableId, currentTable, sql)     // currentTable = KafkaJsonSchema.tableFor
       ├─ 返回 null → 跳过（不改变 schema 的 DDL）
       ├─ applySchemaChange → 改源侧 KafkaJsonSchema
       └─ if isIncludeSchemaChanges:  enqueueSchemaChange
              └─ 构造 schema-change SourceRecord（keySchema.name = io.debezium.connector.kafka.json.SchemaChangeKey）
                 → 入队
  → KafkaJsonEventDeserializer.isSchemaChangeRecord（按 keySchema.name 判断）
  → convertTableChange（CREATE/ALTER/DROP）或 handleRenameTable（见 03）
  → SchemaChangeEvent 子类
```

### 4.2 双解析器（Druid / Debezium ANTLR）

`KafkaJsonDdlParser` 接口 + 可配置双实现，输出统一为 cdc common `SchemaChangeEvent`：

| 解析器 | 实现 | 选择理由 |
|---|---|---|
| `DruidKafkaJsonDdlParser`（默认） | `com.alibaba:druid` 的 `SQLUtils.parseSingleStatement` 解析为 AST（`MySqlCreateTableStatement` / `MySqlAlterTableStatement` / 等），`MySqlSchemaStatVisitor` 提取表名/列/类型/主键/约束 | 阿里系（与 canal 生态同源）、API 直观、社区活跃 |
| `DebeziumAntlrKafkaJsonDdlParser`（备选） | `io.debezium:debezium-connector-mysql` 的 `MySqlAntlrDdlParser`（`optional` 依赖） | 与 flink-cdc 主线其他 connector 一致 |

- 两种解析器**复用 flink-cdc 现成的 MySQL 类型转换**（`MySqlSchemaConverter` / `ColumnConverter`），差异仅在
  "SQL 文本 → 结构化变更"这一层。
- 兼容 canal 的 `type`（CREATE/ALTER/DROP/TRUNCATE/RENAME）。TRUNCATE 映射为 `TruncateTableEvent`（若存在）。
- DDL 解析失败：记录 warn 日志 + 抛 `SchemaOutOfSyncException`（对齐 mysql-cdc 行为）。

### 4.3 列级变更的产出

`KafkaJsonDdlParsedResult` 携带 **before/after 双像** + `KafkaJsonTableChangeType` 枚举
（含 ADD_COLUMN / DROP_COLUMN / ALTER_COLUMN_TYPE / ALTER_COLUMN_COMMENT / ALTER_COLUMN_POSITION 等），
`columnChanges` 列表用于 `KafkaJsonEventDeserializer.diffTable` 做列级 diff。**列改名（RENAME COLUMN）**：
Druid 认 `SQLAlterTableRenameColumn`；pipeline 侧还有**同位置同类型启发式**兜底（旧列消失 + 新列同名位置出现
→ `RenameColumnEvent`，common 原生支持）。

---

## 5. Debezium 消息接入（2026-08 上提执行）

> 承接 ROADMAP P4。原规划文档把这项工作拆成六阶段，评估后大部分内容**已经实现**（列级变更、MessageFormat、
> `scan.message.format`、TiDB watermark 剔除、TSO、Debezium DDL 解析器、端到端 ITCase）；真正新增的是下面的 S1-S4。

### 5.1 评估结论：规划文档哪些过时

| 规划条目 | 现状 |
|---|---|
| 阶段五列级变更（ColumnChangeInfo / 枚举扩展 / Druid parseAlter） | ✅ 已实现（`source/ddl/ColumnChangeInfo.java`、`KafkaJsonTableChangeType` 等） |
| `MessageFormat` 枚举 / 消息格式配置 | ✅ 已存在，但命名是 `scan.message.format=canal|debezium`（原文提议的 `debezium-json.format` 不引入——standard vs ticdc 的差异可由 parser 自动探测，不需要第二个开关） |
| TiDB WaterMark 剔除 | ✅ Canal 格式已剔除 `TIDB_WATERMARK` |
| TiDB TSO → 时间戳 / 快照水印 | ✅ `KafkaJsonTidbOffsetUtils` |
| Debezium DDL 解析器 | ✅ `KafkaJsonDebeziumDdlParser` + `scan.ddl.parser=debezium` |
| 端到端集成测试 | ✅ Phase 11 |

### 5.2 真正的新增点（已落地 S1-S4）

**S1 消息抽象层（最小集）+ 解析器工厂**：
- `EventTime` 加 `TIDB_TSO`；
- `KafkaJsonMessage` 抽象类（`MessageType`：DDL/DML/TIDB_WATERMARK/UNKNOWN）+ `getEventTimeValue(EventTime)`；
- `KafkaJsonMessageParser` 接口 + `KafkaJsonParserFactory.create(MessageFormat)`；
- `CanalMessage extends KafkaJsonMessage`（`KafkaJsonFlatMessage` 改名，与 `DebeziumMessage` 平级）。

> **评估要点**：canal 与 Debezium 的**值表示**不同（canal 是 string 行，Debezium 是 typed struct），不能共用
> 一个"行"模型。所以**不做**统一父类上的 instanceof 大杂烩；保留「接口 + 工厂」的选择层（可插拔的关键），
> 解析器产出各自的消息类型，流式任务只依赖少量公共 getter。

**S2 DebeziumMessage 实体 + DebeziumMessageParser**：
- `DebeziumMessage`（Jackson）：`schema`/`payload`；`payload.source.{db,table,ts_ms,commit_ts,cluster_id}`、
  `payload.{op,before,after,ts_ms,ddl}`；
- 标准格式（schema+payload）、schema-include=false（无 schema 层：原样绑 payload）、
  **TiCDC 自动探测**（`source.commit_ts`/`cluster_id` 存在即 TiCDC）；`op=="m"` → `TIDB_WATERMARK`；
- `getEventTimeValue(ES/TS)` 带跨字段回退（ES→source.ts_ms 缺则取 payload.ts_ms），保证无 source.ts_ms 的
  消息（如裸 schema-change record）仍有排序键。

**S3 流式链路接入**：
- `KafkaJsonStreamFetchTask` 从 `sourceConfig.getMessageFormat()` 建 parser，`processRecords` 用它解析；
- `KafkaJsonRecordConverter` 加 `convert(KafkaJsonMessage, …)` 分派——canal 走现有路径；debezium 走
  `convertDebezium`（typed before/after → `KafkaJsonRecordFactory.debeziumRowData` → `createRecord`，
  **只用已注册表 schema**）；
- `KafkaJsonValueConverter.convertFromJson(Column, JsonNode)`：Debezium 类型化值转换——epoch 编码的时间类型
  （DATE=天、TIME=毫秒、DATETIME=微秒、TIMESTAMP=毫秒）、布尔→Boolean、JSON 列嵌套→compact JSON、
  二进制 base64 文本→byte[]。**DECIMAL 支持 `decimal.handling.mode=precise`**：Kafka Connect
  `JsonConverter` 把该值序列化为 unscaled 字节的 base64 文本（并非 `{"scale","value"}` 对象，scale 在无
  schema 的线格式里不在消息内），按其与列 schema 的 scale 解码为逻辑 `BigDecimal`；`string`（十进制字面量）与
  `double`（数字）同样可消费（三种 mode 的 JSON 呈现均可消费）。
- `KafkaJsonSourceConfigFactory` 放行 `scan.message.format=debezium`。

**实施取舍（已记录）**：
- Debezium DML 依赖已注册表 schema（快照阶段先注册）；**流式-only 无快照场景不适用**——debezium 消息无
  mysqlType，无法 buildTable 兜底，未注册时丢弃并告警。
- 无任何时间戳的 Debezium DDL（裸 `{databaseName,ddl}`）事件时间为 -1，会被低水位过滤丢弃（与 canal DDL 的
  es<low 语义一致）。
- TiDB+Debezium 建议配 `scan.event-time=tidb-tso`：边界与消息事件时间同尺度（TSO 物理毫秒）。

**S4 Pipeline 层打通 + 端到端**：
- 验证 pipeline `KafkaJsonEventDeserializer`（继承 `DebeziumEventDeserializationSchema`）原生消费
  Debezium-shaped SourceRecord，无需改代码；
- `KafkaJsonDebeziumSimulatedChainITCase` 端到端通过：真实 MySQL 快照（注册表 schema）→ 模拟 Debezium 信封 →
  增量 Event 序列与 canal 基线一致。

---

## 6. 真实 CDC 链路（MySQL + 真实 Debezium）

### 6.1 已跑通：`DebeziumCdcChainITCase`（MySQL 8 + Debezium 1.9）✅

infra `DebeziumConnectContainer`：镜像 `debezium/connect:1.9`，`JsonConverter`（schemas 开启 → 产出
`{schema,payload}` 包裹，正是要测的线格式）；connector.class=`io.debezium.connector.mysql.MySqlConnector`，
`snapshot.mode=initial`，主题名 = `{topicPrefix}.{dbName}.{table}`。

ITCase 时序（避免双快照竞态）：
1. MySQL + Kafka + Debezium 起；表已建好含 N 行；
2. 注册 connector → Debezium 全量快照 → **等 Kafka 主题攒够 N 条 `op:r` 记录**（确定性"Debezium 就绪"信号）；
3. 启动 source：JDBC 快照 N 行 → N 个 CreateEvent；流边界取快照后的 Kafka 位置（Debezium 快照记录在边界前，被排除）；
4. 库内执行 DML → Debezium 实时写 `op:c/u/d` → source 消费 M 条；
5. 断言：N 快照 CreateEvent + M 流事件。

`KafkaJsonSourceInfoStructMakerTest` 断言 source struct `version == "1.9.8.Final"`，与真实 producer 的
`debezium/connect:1.9` 线格式一致。

**DECIMAL 线格式 e2e（同 ITCase 的 `testRealDebeziumPreciseDecimalWireFormat`）**：单独建一张
`orders`（`DECIMAL(10,2)`，避开共享 customers fixtures），断言 Debezium 写出的 `3.14` 在 Kafka 上是
**base64 文本 `"ATo="`**（非 `{scale,value}` 对象），且 JDBC 快照行与增量 `op:c`（`25.50`）都被连接器解成
正确的 `DECIMAL(10,2)` DecimalData——即 §S3 的 precise 解码在真实链上成立。

### 6.2 已放弃：PolarDB-X 链路（记录卡点）

`PolardbXChainITCase`（PolarDB-X → canal → Kafka）已尝试并**移除**。卡点：`polardbx/polardb-x:v2.4.2_5.4.19`
的 CN 分布式 DDL 引擎，在 testcontainers 启动的容器里 `CREATE DATABASE` 的物理 DDL job 不落地——逻辑库建出、
DN 物理库缺失，后续 `CREATE TABLE` 永远报误导性的 `ERROR 1046 No database selected`。

排查已逐项排除：裸 `docker run` 同 SQL 4/4 一次过（非网络/端口/JDBC/内存）；testcontainers 内 DDL 就绪探针
建删成功（容器环境不阻止 DDL 引擎本身）；差异收窄到 testcontainers 传给 docker 的容器参数，未进一步定位。

**结论**：PolarDB-X 镜像 CN 的 DDL 行为在 testcontainers 环境不稳定，属被测库镜像缺陷，非连接器代码问题。
若日后重启此链：先在失败时 dump CN 的 `/logs/tddl/ddl*.log` 定位真实物理 DDL job 报错。

---

## 7. 验证方式

```bash
# 真实 Debezium 链路
mvn -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json test -Dtest=DebeziumCdcChainITCase -DfailIfNoTests=false
```

> 全绿 → 提交 push。任一真实链路暴露格式差异 → 如实报告，回计划层决策，不静默掩盖。

---

## 8. 四类生产者 × 事件 → SourceRecord → Event 全矩阵（基于真实抓包）

本节是"从 Kafka 上的一条字节，到下游拿到的一个 Event"的**全路径地图**。每一条结论都来自
`src/test/resources/kafkajson/captured/` 里的真实抓包（抓取方法见该目录的 `README.md`），
由 `KafkaJsonCapturedMessageTest`（source 模块）与 `KafkaJsonRenameFixtureTest`（pipeline 模块）回放。

### 8.1 生产者与版本（抓包实测）

| 生产者 | 版本 | 格式开关 | DDL 落在哪 | 抓包目录 |
|---|---|---|---|---|
| canal-server | 1.1.8 | `scan.message.format=canal` | 同一个 topic（`isDdl=true`） | `captured/mysql-canal/`、`captured/mysql-canal-swap/` |
| TiCDC | 8.5.1 | `canal`（`canal-json` 协议） | 同一个 topic | `captured/tidb-canal/` |
| Debezium MySQL | 1.9.7（Kafka Connect 1.9） | `debezium` | **schema-history topic**，不在数据 topic 上 | `captured/mysql-debezium/` |
| TiCDC | 8.5.1 | `debezium`（`enable-tidb-extension`） | **完全没有 DDL**（8.5.1 只发 DML） | `captured/tidb-debezium/` |

### 8.2 全路径流程图

```
 ┌─ 生产者 ────────────────┐   ┌─ 消息 ────────────────────┐   ┌─ 解析 ─────────────────────┐   ┌─ SourceRecord ──────────┐   ┌─ Event ─────────────────┐
 │ canal-server 1.1.8      │   │ flatMessage isDdl=false   │──▶│ CanalMessageParser         │──▶│ KafkaJsonRecordConverter │──▶│ DataChangeEvent         │
 │ (MySQL, flat message)   │   │  (data[]/old[]/pkNames)   │   │  → CanalMessage            │   │  → DML 形状 SR           │   │                         │
 │                         │   ├───────────────────────────┤   ├────────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
 │                         │   │ flatMessage isDdl=true    │──▶│ CanalMessageParser         │──▶│ KafkaJsonSchemaChange-   │──▶│ CreateTableEvent        │
 │                         │   │  (sql/table/database)     │   │                            │   │ Handler.handle           │   │ AddColumn/DropColumn/…   │
 │                         │   ├───────────────────────────┤   ├────────────────────────────┤   │  ├ 改 L1 注册表          │   │ RenameTableEvent        │
 │                         │   │ type=QUERY, isDdl=false,  │──▶│ 丢弃（0 条记录）            │   │  └ 入队 schema-change SR │   │ Truncate/DropTableEvent │
 │                         │   │   data=null（ROWS_QUERY） │   │                            │   │                         │   │                         │
 │                         │   ├───────────────────────────┤   ├────────────────────────────┤   │                         │   │                         │
 │                         │   │ type=QUERY, isDdl=true    │──▶│ 按 DDL 解析（如            │   │                         │   │                         │
 │                         │   │   （DDL 也复用 QUERY）     │   │   DROP DATABASE）           │   │                         │   │                         │
 ├─────────────────────────┤   ├───────────────────────────┤   ├────────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
 │ TiCDC 8.5.1             │   │ canal-json DML            │──▶│ 同上（table = 新名）        │──▶│ 同上                     │──▶│ 同上                     │
 │ (MySQL/TiDB, canal-json)│   ├───────────────────────────┤   ├────────────────────────────┤   │                         │   │                         │
 │                         │   │ canal-json DDL            │──▶│ 同上；多对 rename 已被      │   │                         │   │                         │
 │                         │   │                           │   │ TiCDC 拆成一条一对          │   │                         │   │                         │
 ├─────────────────────────┤   ├───────────────────────────┤   ├────────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
 │ Debezium MySQL 1.9.7    │   │ 数据 topic envelope       │──▶│ DebeziumMessageParser      │──▶│ DebeziumRecordConverter  │──▶│ DataChangeEvent         │
 │ (Kafka Connect 1.9)     │   │  (op/before/after/source) │   │  → DebeziumMessage         │   │  → DML 形状 SR           │   │                         │
 │                         │   ├───────────────────────────┤   ├────────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
 │                         │   │ schema-history record     │──▶│ DebeziumMessageParser      │──▶│ KafkaJsonSchemaChange-   │──▶│ CreateTableEvent        │
 │                         │   │  (ddl + tableChanges)     │   │  → DebeziumMessage         │   │ Handler.handle           │   │ RenameTableEvent        │
 ├─────────────────────────┤   ├───────────────────────────┤   ├────────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
 │ TiCDC 8.5.1             │   │ 仅 DML envelope           │──▶│ DebeziumMessageParser      │──▶│ DebeziumRecordConverter  │──▶│ DataChangeEvent         │
 │ (debezium 协议)          │   │  (source.commit_ts)       │   │  → TiCDC 自动探测           │   │                          │   │  ⚠ 无 DDL：建表事件只能   │
 │                         │   │                           │   │                            │   │                          │   │  来自 JDBC 快照          │
 └─────────────────────────┘   └───────────────────────────┘   └────────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘
                                                                          │
                                                                          ▼
                                                        KafkaJsonEventDeserializer（pipeline 模块）
                                                          isSchemaChangeRecord? → 按 keySchema.name
                                                          ├ 数据记录 → 父类 DebeziumEventDeserializationSchema
                                                          ├ tableChangeType=RENAME_TABLE → handleRenameTable
                                                          ├ tableChangeType=TRUNCATE_TABLE → TruncateTableEvent
                                                          └ 其余 → convertTableChange（列级 diff）
```

### 8.3 逐事件矩阵（DML / CREATE / ALTER / RENAME / TRUNCATE / DROP）

| 事件 | canal-server（flat） | TiCDC（canal-json） | Debezium 1.9.7（MySQL） | TiCDC（debezium） |
|---|---|---|---|---|
| **DML** | `data[]` 批（多行一条消息），值全 String，类型靠注册表 | 同 canal，`mysqlType` 全小写 | 数据 topic，typed `before`/`after`，`op` c/u/d/r | 同 Debezium + `source.commit_ts` |
| **CREATE** | `isDdl=true,type=CREATE`，`table` = 新表名 | 同 canal | schema-history record，`tableChanges[0].type=CREATE` | **无** |
| **ALTER** | `type=ALTER`；列级变更由 Druid AST + 前后 schema 双像 diff | 同 canal | schema-history record，`tableChanges[0].type=ALTER`（**只有后像**） | **无** |
| **RENAME** | 一条消息含**全部 pairs**，`table` = **第一对的 new 名**；`ALTER … RENAME TO` 也报 `type=RENAME` | **每对一条消息**，SQL 被 TiCDC 重写成单对语句；`table` = new 名 | **每对一条记录**，`ddl` 被重写成单对；`source.table` 是**整条语句所有名字的逗号串**（`"t2_new,t1_new"`），根本不是表名 | **无** |
| **TRUNCATE** | `type=TRUNCATE` | 同 canal | schema-history record（TRUNCATE 不改变 schema，靠类型标记送下游） | **无** |
| **DROP** | `type=ERASE`（实测唯一取值，不是 `DROP`） | 同 canal | schema-history record | **无** |
| **ROWS_QUERY** | `type=QUERY` + `isDdl=false`：`data=null`、`database=""`、`sql` 是原始 DML 文本 → **0 条记录**（`binlog_rows_query_log_events=ON` 时每条 DML 前都有一条） | 无 | 无 | 无 |
| **QUERY（DDL）** | `type=QUERY` + **`isDdl=true`**（`DROP DATABASE IF EXISTS …` 也走这个 type）→ 按 DDL 解析 | 同 canal | 无 | 无 |
| **水位** | — | `TIDB_WATERMARK`（`op=m` 走 Debezium 格式时） | — | `op=m` → `TIDB_WATERMARK` |

### 8.4 三条硬结论（改名场景）

1. **rename 的前后表名一律取自 DDL SQL**：四种生产者的 `table` 字段对 rename 全都不可用
   （新名 / 第一对的新名 / 逗号串 / null）。这是 `KafkaJsonDruidDdlParser.parseRenameTable` 与
   `KafkaJsonDebeziumDdlParser` 都从语句取 pairs 的原因。
2. **一条语句 = 一个事件**（能拿到时）：canal-server 一条消息携带全部 pairs，因而下游能看到
   "名字成环"，才能用 Doris 的原子 `REPLACE WITH TABLE … swap=true` 表达对调。TiCDC 与 Debezium
   把多对语句**拆开**，下游就只看到一串单对 rename——**依然正确**（按语句顺序逐条执行、临时名是真实
   存在的表），但失去原子性：中途失败会停在临时名上。详见 [03-event-model.md](./03-event-model.md) §3。
3. **无 DDL 的生产者（TiCDC debezium）**：建表/改表事件只能来自 JDBC 快照阶段，增量阶段没有 schema
   变更可消费——部署时选 `canal-json` 而不是 `debezium`，否则流中的 DDL 不可见。
