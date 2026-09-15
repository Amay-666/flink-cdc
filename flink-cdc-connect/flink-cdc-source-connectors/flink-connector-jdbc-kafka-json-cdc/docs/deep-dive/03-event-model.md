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
- `RenameTableEvent.getType()` **抛 `UnsupportedOperationException`**（不是返回占位值）。released 的
  `SchemaChangeEventType` 枚举里没有 RENAME_TABLE，与其返回一个错的值让"按 getType 走 generic 逻辑"
  的代码默默走错分支，不如直接炸掉。自定义序列化栈按 class 分派，**不调用 `getType()`**，因此不受影响。
  **边界：别把 RenameTableEvent 喂给 released 的 `SchemaManager`/`SchemaDerivation`/`EventSerializer`**，
  它们不认这个类（会 throw）。本部署模型（自建序列化栈）不会经过它们。

---

## 3. RENAME_TABLE 全链路（Plan A 核心新增）

### 3.1 一条原则：前后表名一律取自 DDL SQL

生产者给的"表名"字段对 rename **全部不可用**：canal/TiCDC 的 `table` 是**改名后**的名字，Debezium 的
`source.table` 是整条语句所有名字的逗号串（`"a,a_tmp,b"`），TiCDC 的 `debezium` 协议连 DDL 都不发。
所以 `KafkaJsonDruidDdlParser` / `KafkaJsonDebeziumDdlParser` 都从 **SQL 语句本身**解析出 pairs，
`message.getTable()` 只用来做兜底校验。实测矩阵见 [02-message-parsing.md](./02-message-parsing.md) §8。

### 3.2 全链路

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
  │ 命中 MySqlRenameTableStatement / SQLAlterTableRename
  │   → parseRenameTable：遍历 items，**每一对**产出一个 RenamePair（不再截断成 items[0]）
  │ （Debezium 版：KafkaJsonDebeziumDdlParser 同样从 SQL 解析；findRenamedTable 的
  │   "旧名消失 + 新名出现" diff 降级为校验/兜底，因为多对时它不可靠）
  ▼
KafkaJsonDdlParsedResult.renameTable(pairs)               type = RENAME_TABLE
                                                  [source/ddl/KafkaJsonDdlParsedResult.java]
  │ 每对含 oldTableId / newTableId / oldTable / newTable
  ▼
KafkaJsonSchemaChangeHandler.applyRename                    ── 组装"净改名" ──
  │   按语句顺序逐对：tableFor(old) → removeTable(old) + registerTable(new)  【L1 状态改】
  │   并把"经临时名中转"的对折叠进起始那一次改名（见 3.3）
  │   tableFor(old) == null → **fail-fast**（见 3.4）
  ▼
KafkaJsonSchemaChangeHandler.enqueueSchemaChange
  │   tableChanges.create(每个 newTable 一个 CREATE change，与 pairs 同序)
  │   historyDocument.set(tableChangeType, "RENAME_TABLE")
  │   historyDocument.set(renamePairs, [{oldTableId, newTableId}, …])   // 有序
  │   → schema-change SourceRecord 入队
  ▼
KafkaJsonEventDeserializer.deserializeSchemaChangeRecord     [pipeline source/KafkaJsonEventDeserializer.java]
  │ isRenameTableChange?(historyRecord)  → handleRenameTable
  │   pairs ← historyRecord.renamePairs（**每个 id 都来自这里**）
  │   schema ← 按序 zip tableChanges 里的 CREATE change      // 不再用 record.source.db/table 拼旧名
  │   tables.removeTable(old…) + tables.overwriteTable(new…)  【L2 状态改】
  │   sql ← historyRecord 的 DDL_STATEMENTS（原始整条语句）
  ▼
RenameTableEvent(pairs, sql)                               [pipeline event/RenameTableEvent.java]
  │ getPairs() 有序；tableId()/getSchema() = pairs[0]（与单对路径完全一致）
  ▼
下游算子：instanceof RenameTableEvent → 逐对迁移自己的状态 old→new
        [pipeline example/KafkaJsonRenameStateOperator.java]    【L3 状态改，必须由下游做】
  ▼
Doris sink：pairs 成环（a→b, b→a）→ 一条原子 REPLACE WITH TABLE … swap=true
          否则逐条 ALTER TABLE … RENAME（见 05-doris-sink.md §4）
```

**自定义字段**（`KafkaJsonSchemaChangeHandler` 常量，被 deserializer 读取）：

| 字段 | 值 | 含义 |
|---|---|---|
| `tableChangeType` | `"RENAME_TABLE"` / `"RENAME_COLUMN"` / `"TRUNCATE_TABLE"` / … | 标记这是 Debezium TableChangeType 表达不了的改动 |
| `renamePairs` | `[{"oldTableId":"db.a","newTableId":"db.b"}, …]` | RENAME_TABLE 的**有序** pairs（`oldTableId`/`newTableId`），是 deserializer 唯一的前后名来源 |

> 早期版本用单个 `canalNewTableId` 字段、旧名从 `record.source.table` 拼——那正是"canal 的 `table` 是新名"
> 这个 bug 的载体（旧名拿到的是新名，`oldTableId == newTableId`，Doris 侧生成 `ALTER TABLE db.x RENAME x`
> 自改名）。字段换成 pairs 数组后这条路径不存在了。**序列化格式随之变化 → 旧 checkpoint 不兼容**（同分支未发布）。

### 3.3 净改名：多对语句里"经临时名中转"的对要折叠

MySQL/TiDB **唯一**能写出两表对调的形式是临时名三步走（`RENAME TABLE a TO b, b TO a` 直接报
`ERROR 1050 Table 'b' already exists`，已实测）：

```sql
RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b
```

`a_tmp` 只在语句内部存在过。若把三对原样发给下游，Doris 会收到 `ALTER TABLE db.a_tmp RENAME b` ——
一张它从未见过的表，执行失败并在库里留下野表 `a_tmp`。所以
`KafkaJsonSchemaChangeHandler.applyRename` 按语句顺序扫描 pairs，用一张
"名字 → 已报告 pair 的下标"表把中转折叠掉：某对的 old 名如果正是前面某对刚留下的名字，它就不是新的一次
改名，而是**那一次改名的继续**——保留原 old 的 schema 与原 old id，只把目标名换成这一对的 new 名。

上面那条语句的结果是**两对**：`a→b`（schema 用 a 的）、`b→a`（schema 用 b 的）。下游于是看到的就是
"这两个名字对调了"，正好是 Doris 一条 `REPLACE WITH TABLE … PROPERTIES('swap'='true')` 能原子表达的语义。
原始 SQL 仍原样保留在 history record 的 DDL 字段里，供排查。

**代价（如实记录）**：生产者把多对语句拆开发（TiCDC、Debezium 一对一条），下游就只能看到一串单对
rename——**依然正确**（按语句顺序逐条执行，中转名是真实存在过的表），但失去原子性：中途失败会停在
`a_tmp` 上。canal-server 一条消息携带整条语句，因而能拿到原子性。这是生产者决定的，连接器无法弥补。

### 3.4 旧表 schema 未知 → fail-fast

`applyRename` 里 `tableFor(oldTableId) == null` 直接抛异常，而不是"注册一个空 schema 的表"。
理由：rename 之前从未观测到该表的 CREATE（job 起点在 CREATE 之后、或 Kafka 起始位点太靠后），
继续下去这张表会以 0 列注册，之后它的数据事件会被解析成 0 列的行**静默写坏数据**。
异常信息里直接写清补救办法：把 job（或 Kafka 起始位点）调到能看到该表 CREATE 之前。

### 3.5 跨库改名

source 侧**如实解析**（`KafkaJsonRenameTableIds`：不带库名的一方继承语句的 default database），
不去猜、不静默改写。表达不了这一限制落在 Doris sink：`DorisDdlBuilder.buildRenameTableSql` 对每一对
比较 `mapDatabase(old)` 与 `mapDatabase(new)`，不同就抛 `UnsupportedOperationException` 并给出两条出路
（把两个 id 映射到同一个 Doris 库，或在源端分两步经临时名改名）。

注意判据是**映射后**的库名而非源库名：一个库映射可以把两个源库并进一个 Doris 库，也可以把一个源库拆到
几个 Doris 库；Doris 的 `ALTER TABLE db.a RENAME b` 与 `REPLACE WITH TABLE b` 的第二个名字都只在第一个
表的库里解析（实测）。

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

测试资源固化在 `src/test/resources/kafkajson/`：
`captured/` 下是**真实抓包**（逐字节原样、永不修改，抓取方法见其中的 `README.md`），其余是按 wire format
手写的样本（是"假设"，不是事实；被抓包推翻时保留原样作为护栏）。目录说明与"生产者 × rename 行为"实测
矩阵见 `src/test/resources/kafkajson/README.md`。典型形状：

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
