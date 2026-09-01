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

# 事件模型与序列化栈（含 RENAME 事件 Plan A）

> 本文讲连接器输出给下游的 **Event 流**：有哪些事件类型、序列化栈如何自包含地支持自定义事件、
> 事件发射顺序、以及 RENAME TABLE 的完整链路（Plan A）。
> 关联：[02-message-parsing.md](./02-message-parsing.md)（SourceRecord 从哪来）、[04-ddl-blocking.md](./04-ddl-blocking.md)（pipeline 侧事件消费）。

---

## 1. 事件类型树

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

5 个标准事件（released 认识）由 common 提供；`RenameTableEvent` 是 pipeline 模块自建的新事件
（连接器的 5 个自定义事件还包括 DropTable / TruncateTable / AlterTableComment / AlterColumnComment，
见 [05-doris-sink.md](./05-doris-sink.md) 与 [04-ddl-blocking.md](./04-ddl-blocking.md)）。

---

## 2. 序列化栈（自包含，不碰 released）

```
KafkaJsonEventDeserializer.getProducedType() → new KafkaJsonEventTypeInfo()     ← 唯一接缝
  └─ createSerializer() → KafkaJsonEventSerializer.INSTANCE                 [serializer/KafkaJsonEventSerializer.java]
        （released EventSerializer 的本地副本；SchemaChangeEvent 委托给 KafkaJsonSchemaChangeEventSerializer）
        └─ KafkaJsonSchemaChangeEventSerializer.INSTANCE                     [serializer/KafkaJsonSchemaChangeEventSerializer.java]
             instanceof 分派：
             ├─ 5 个 released per-event serializer（字节格式与 released 一致）
             └─ RENAME_TABLE → KafkaJsonRenameTableEventSerializer.INSTANCE   [serializer/KafkaJsonRenameTableEventSerializer.java]
```

**唯一接缝**是 `KafkaJsonEventDeserializer.getProducedType()`。换序列化栈只需换它返回的 `TypeInfo`。

### 2.1 字节格式与"关闭分派"（必读）

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

## 3. RENAME_TABLE 全链路（Plan A 核心新增）

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

**列改名（RENAME_COLUMN）**：`KafkaJsonDruidDdlParser` 认 `SQLAlterTableRenameColumn`；pipeline 侧
`KafkaJsonEventDeserializer.diffTable` 还有**同位置同类型启发式**兜底（旧列消失 + 新列同名位置出现 →
`RenameColumnEvent`，common 原生支持，零公共 API 改动）。

---

## 4. 状态归属（三层）与数据处理顺序

### 4.1 三层状态，各管各的

| 层 | 类 | 状态 | 谁改 | 用途 |
|---|---|---|---|---|
| **L1 源侧** | `KafkaJsonSchemaChangeHandler.applySchemaChange` → `KafkaJsonSchema` | source 侧"当前 schema 记忆" | 每条 DDL | 快照读表（`KafkaJsonScanFetchTask:78`）、`tableFor` 给 ALTER parser |
| **L2 反序列化侧** | `KafkaJsonEventDeserializer` 的瞬态 `Tables` | 注册表 | CREATE/RENAME/DROP 时 | **仅** ALTER 列级 diff（old vs new） |
| **L3 下游** | **你的算子**（如 `KafkaJsonRenameStateOperator`） | 按 TableId 的业务状态 | 收到 CreateTableEvent/RenameTableEvent | 最终消费状态 |

**关键结论**：
1. L1/L2 都是连接器内部工作寄存器，**不会传给下游**。
2. **数据事件的 tableId 不来自任何注册表**，来自 SourceRecord `source` 结构的 `db`/`table`
   （`KafkaJsonEventDeserializer.getTableId`）。`RENAME TABLE` 之后 binlog/canal 消息的 table 就是新名，
   所以**后续 DataChangeEvent 天然带新名**，不需要状态参与。
3. 下游的"新表加入"（CREATE）和"改名迁移"（RENAME）**必须由下游自己处理**——连接器只发事件。
   这就是 `KafkaJsonRenameStateOperator` 存在的意义（`perTableState.remove(old).put(new)`）。

### 4.2 事件发射顺序（下游会看到的顺序）

- **INITIAL 启动**：快照阶段每个 split 的 **LOW watermark** 时，`KafkaJsonPipelineRecordEmitter` **惰性**发
  该 split 表的 `CreateTableEvent`（`alreadySendCreateTableTables` 去重，避免 checkpoint 超时）；
  快照→增量切换（stream split 开始）时，把缓存中**未发过**的 `CreateTableEvent` 一次性补发
  （用缓存 schema 而非重新查库，避免比排队中的 schema-change 事件新）。
- **纯流式（非 initial）**：stream split 开始把全部 `CreateTableEvent` 一次发完。
- **流中 DDL**：按 Kafka 顺序进入队列，`KafkaJsonEventDeserializer` 按序转成各种 `SchemaChangeEvent`。
- **数据事件**：紧跟其后，tableId 已是最新。

> 注意：`CreateTableEvent` 有两个来源——(a) 惰性/缓存补发（从 JDBC schema 取），(b) 流中 DDL CREATE
> （从 history record 取）。**同表 id 只会发一次**（已发表集合 + DDL 只在实际 CREATE 时产生）。下游按
> 幂等处理更稳。

### 4.3 已知限制（ALTER 对"流中未见 CREATE"的表）

L2 注册表**只被流中的 CREATE schema-change 记录填充**（快照阶段惰性发的 CreateTableEvent 不经过
`convertTableChange`）。所以：**job 启动前就存在的表，之后来了 ALTER，L2 里没有旧表 → diff 为空 →
不发列级事件**（deserializer `convertTableChange` ALTER 分支 `oldTable==null` → skip）。这是有意的
保守行为（避免对已存在表发 CreateTableEvent 被下游 SchemaManager 拒绝）。

---

## 5. 补充：canal flatMessage 样本（测试资源基准）

测试资源固化在 `src/test/resources/canal/`。典型形状：

**INSERT**
```json
{"id":1,"database":"test","table":"users","pkNames":["id"],"isDdl":false,"type":"INSERT",
 "es":1598752886000,"ts":1598754586044,"sql":"",
 "sqlType":{"id":4,"name":12},"mysqlType":{"id":"int(11)","name":"varchar(255)"},
 "data":[{"id":"1","name":"Alice"}],"old":null}
```

**DDL / CREATE**
```json
{"id":4,"database":"test","table":"orders","pkNames":null,"isDdl":true,"type":"CREATE",
 "es":1598752889000,"ts":1598754586077,"sql":"create table orders(id int not null auto_increment primary key, amount decimal(10,2), created_at datetime)",
 "sqlType":null,"mysqlType":null,"data":null,"old":null}
```

> canal 版本差异：`type` 枚举可能含 `QUERY`/`TRUNCATE`/`RENAME`/`ERASE`；`isDdl` 恒为 `true` 的是 DDL；
> `data`/`old` 可能为 `null`；消息 key 可为空（连接器不需要 key，主键从 `data`/`old` 取）。
