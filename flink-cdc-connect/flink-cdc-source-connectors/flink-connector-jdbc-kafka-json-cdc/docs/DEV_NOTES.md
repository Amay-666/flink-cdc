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

# 开发注意事项（坑、约束、扩展指南）

> 分支：`feature/canal-rename-plan-a`
> 本文档是给维护者看的**速查手册**：构建测试命令、checkstyle 规则、类型与命名坑、设计约束、扩展模板。
> 具体数据流 / 事件模型 / coordinator 原理见各 [deep-dive/](./README.md) 子文档。

---

## 1. 构建与测试命令

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

> **pipeline 模块必须用 `-am test`**：它依赖 `flink-connector-jdbc-kafka-json-cdc:test-jar`，
> 本地 m2 没有 canal artifacts，`compile`/`install` 都失败（缺 jar-plugin 传递依赖），
> 只有 `-am` 在 reactor 里构建并处理 test-jar。

---

## 2. Checkstyle 规则

**别加 `-Dcheckstyle.skip=true`**，会漏掉 import 顺序错误。

- 分组：`org.apache.flink, org.apache.flink.shaded, *, javax, java, scala`，**组间空行**。
- `*` 组把 `io.debezium` + `org.apache.kafka` + `org.junit` + `org.assertj` 等合并成一个字母序块，**块内无空行**。
- 但 `org.apache.flink` 组与 `*` 组之间**必须有空行**——新文件最容易漏这个。

---

## 3. 类型与命名坑

1. **`Schema` 全限定**：`KafkaJsonEventDeserializer` 里已 import kafka 的 `org.apache.kafka.connect.data.Schema`，
   所以 common 的 schema 必须写全限定 `org.apache.flink.cdc.common.schema.Schema`。
2. **`KafkaJsonEventTypeInfo` 必须重写 `equals`/`hashCode`**（`instanceof KafkaJsonEventTypeInfo` + `getClass().hashCode()`）。
   released `EventTypeInfo` 的 equals 是 `instanceof` 而 hashCode 按类名，子类不改会违反 equals/hashCode 契约。
3. **`TableChanges.drop(Table)` 收的是 Table 不是 TableId**——DROP 记录要 `Table.editor().tableId(id).create()`。
4. **`VARCHAR(0)` 抛异常**：测试里建 VARCHAR 列 `length` 必须 > 0。
5. **`Document.toJson()` 不存在**：用 `DocumentWriter.defaultWriter().write(document)` 生成 historyRecord 字符串。
6. **`OperationType` 是顶层类** `org.apache.flink.cdc.common.event.OperationType`（不是嵌套）。
7. **`KafkaJsonDdlParsedResult` 没有 `getTable()` alias**——用 `getNewTable()`（历史遗留，别写错）。

---

## 4. 设计约束（务必遵守）

1. **released serializer 是"关闭分派"**（`instanceof` + `else throw`）：新事件必须复制一份序列化器到本地
   （pipeline `serializer/` 包），用**自己的 tag 枚举**，不能往 released 加 case。
2. **`RenameTableEvent.getType()` 占位值 CREATE_TABLE**：只影响 generic 代码；自定义序列化栈按 class 分派
   不受影响。**别把它喂给 released SchemaManager/SchemaDerivation/EventSerializer**（会 throw 或误判为 CREATE_TABLE）。
3. **`include.schema.changes` 默认 false**：关闭时 DDL 只改源侧 `KafkaJsonSchema`，**不发 schema-change 记录**
   （下游看不到 CreateTableEvent/RenameTableEvent）。你的 job 里开起来才能感知 rename。
4. **数据事件 tableId 来自 source 结构**，不来自 L1/L2 注册表——验证/单测时 source 的 db/table 字段要写对。
5. **下游状态不自动迁移**：RenameTableEvent 只是通知，迁移必须由下游算子做。
6. **`KafkaJsonSchema` 快照用途**：`KafkaJsonScanFetchTask` 把 `getDatabaseSchema()` 传给 `KafkaJsonSnapshotSplitReadTask`
   （快照读表用），别在 DDL handler 之外乱动它。
7. **JDK17 + Flink 测试 harness 冲突**：`ProcessFunctionTestHarnesses`/`OneInputStreamOperatorTestHarness`
   在 JDK17 触发 Chill/Kryo 反射 `Arrays$ArrayList` 的 `InaccessibleObjectException`（root pom surefire
   没有 `--add-opens`）。**参考算子测试直接驱动 `open()` + `processElement()`**（该算子 open 不需要
   RuntimeContext、processElement 不用 Context），不碰 harness。

---

## 5. 扩展指南（模板）

### 5.1 新增一种 SchemaChangeEvent（如显式 DROP_TABLE 事件）

要动的文件（全在 pipeline 模块，released 零改动）：

1. `event/XxxEvent.java`——复制 `RenameTableEvent` 模板（`implements SchemaChangeEvent`，字段 + equals/hashCode/toString）。
2. `serializer/KafkaJsonXxxEventSerializer.java`——复制 `KafkaJsonRenameTableEventSerializer` 模板。
3. `serializer/KafkaJsonSchemaChangeEventSerializer.java`——`KafkaJsonSchemaChangeTag` 加一个值；`copy`/`serialize`/`deserialize`
   三个方法各加一个 `instanceof`/`case` 分支。
4. 产出点：`KafkaJsonEventDeserializer` 里对应分支 `new XxxEvent(...)`。
5. 测试：`KafkaJsonEventSerializerTest` 加 round-trip；`KafkaJsonEventDeserializerTest` 加产出来源。

### 5.2 新增 DDL 识别（如 `ALTER TABLE ... PARTITION BY`）

1. `source/ddl/KafkaJsonDruidDdlParser.java`——加 Druid AST 类型分支；`KafkaJsonDebeziumDdlParser.java`——加 ANTLR 分支。
2. `source/ddl/KafkaJsonTableChangeType.java`——加枚举值；`KafkaJsonDdlParsedResult.java`——加工厂方法。
3. `source/handler/KafkaJsonSchemaChangeHandler.java`——`applySchemaChange` 和 `enqueueSchemaChange` 加 type 分支
   （以及需要的话新 custom history-record 字段）。
4. `KafkaJsonEventDeserializer`——加对应事件产出。

### 5.3 修改数据事件字段（DataChangeEvent 结构）

`DataChangeEventSerializer` 是 released 的（不能改）。做法：复制到 pipeline `serializer/` 包成为
`KafkaJsonDataChangeEventSerializer`，在 `KafkaJsonEventSerializer` 里替换引用。**同一个 job 内新旧序列化器字节
不兼容**，改前想清楚是否需要兼容旧 checkpoint。

### 5.4 常见改动点速查

| 想做什么 | 改哪 |
|---|---|
| 换事件序列化栈 | `KafkaJsonEventDeserializer.getProducedType()` |
| 改 DDL 识别 | `source/ddl/*Parser` + `KafkaJsonDdlParsedResult` + `KafkaJsonSchemaChangeHandler` |
| 改源侧状态 | `KafkaJsonSchema`（registerTable/removeTable） |
| 改 ALTER 列级 diff | `KafkaJsonEventDeserializer.diffTable` |
| 下游感知 rename | 你的算子 + `KafkaJsonRenameStateOperator` 参考 |
| 快照阶段灌 L2 注册表 | `KafkaJsonPipelineRecordEmitter` 或 `KafkaJsonEventDeserializer` 构造点 |
| 改 pipeline 的 DDL 阻塞行为 | 见 [deep-dive/04-ddl-blocking.md](./deep-dive/04-ddl-blocking.md) |
| 改 Doris 写入 / DDL 执行 | 见 [deep-dive/05-doris-sink.md](./deep-dive/05-doris-sink.md) |
