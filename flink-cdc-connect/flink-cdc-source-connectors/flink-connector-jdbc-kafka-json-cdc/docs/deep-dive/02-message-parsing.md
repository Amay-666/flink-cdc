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
              └─ 构造 schema-change SourceRecord（keySchema.name = io.debezium.connector.canal.SchemaChangeKey）
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
- `KafkaJsonValueConverter.convertFromJson(Column, JsonNode)`：De bezium 类型化值转换——epoch 编码的时间类型
  （DATE=天、TIME=毫秒、DATETIME=微秒、TIMESTAMP=毫秒）、布尔→Boolean、JSON 列嵌套→compact JSON、
  二进制 base64 文本→byte[]。**DECIMAL 不支持 `decimal.handling.mode=precise`（base64 字节）**，用 double/string 或 TiCDC。
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
