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

# 自写 coordinator：多并行度下的 DDL 阻塞与确认

> 本文回答一个问题：**schema 变更（DDL）发生的那一刻，多个并行度的 sink 一边还在往旧表结构写数据，
> 怎么保证"先让所有旧数据落盘，再执行 DDL，再放行新结构的数据"？**
> 关联：[03-event-model.md](./03-event-model.md)（事件从哪来）、[05-doris-sink.md](./05-doris-sink.md)（DDL 执行到哪里）、
> [01-exactly-once.md](./01-exactly-once.md)（checkpoint 与数据一致性）。
> 与 released 的逐文件对照见 `../coordinator-diff/`（归档在 pipeline 模块 docs 下）。

---

## 1. 为什么 coordinator 必须自写

flink-cdc 的 DDL 协调中心（`SchemaRegistry` coordinator）在 released 里是**按事件类型关闭分派**的：
`instanceof` 只认识 5 个标准 `SchemaChangeEventType`，连接器的 5 个自定义事件
（RenameTable / DropTable / TruncateTable / AlterTableComment / AlterColumnComment）在以下环节全部会崩：

| released 环节 | 崩法 |
|---|---|
| `SchemaRegistryRequestHandler.handleSchemaChangeRequest` | behavior 判断前先调 `isOriginalSchemaChangeEventRedundant`，对未知类走默认分支 |
| `SchemaChangeEventSerializer` / `EventSerializer.copy` | `PrePartitionOperator.broadcastEvent` 深拷贝时 `throw IllegalArgumentException` |
| `PartitioningEventTypeInfo` / `EventTypeInfo` | 分区段网络传输序列化，自定义 tag 无 case |

因此 coordinator、分区算子链（涉及自定义事件的部分）、序列化栈**全部自写**。
复用的是「事件类型无关」的类：协调 RPC DTO、`SchemaEvolutionClient`、`DataSinkWriterOperatorFactory`、
`FlushEvent`（common）、`PartitioningEvent` / `EventPartitioner` / `PartitioningEventKeySelector` /
`PostPartitionProcessor`、`DefaultDataChangeEventHashFunctionProvider`（按「表名+主键」hash）。

> **为什么算子不用自写**：released `SchemaOperator` 逐行确认只在 **EXCEPTION** behavior 下调用
> `getType()`（`SchemaOperator.java:430`）；连接器固定 **EVOLVE**，该分支短路安全。rename 后新 tableId 的
> 数据走 `schemaDivergesMap.getIfPresent == null` 的 passthrough 分支，不触发 normalize。所以只自写
> `KafkaJsonSchemaOperatorFactory`（`getCoordinatorProvider` 返回自写 Provider），算子里层用 released。

---

## 2. 阻塞-确认协议（状态机）

连接器仿 released `SchemaRegistryRequestHandler` 的状态机：

```
SchemaOperator(subtask)              KafkaJsonSchemaRegistry (coordinator, 单线程)         DorisSinkWriter（每并行度）
  收到 SchemaChangeEvent（标准或自定义）
  → SchemaChangeRequest ────────────→  IDLE: 去重(事件类型无关) → 派生 → WAITING_FOR_FLUSH
  ← SchemaChangeResponse.ACCEPTED
  → 下发 FlushEvent（经分区链广播到全部并行度）
                                       ← FlushSuccessEvent（writer.flush() = StreamLoad PUT 完成后上报）
                                         flushed==registered 且 ≥ 并行度 → APPLYING
                                         → DorisMetadataApplier.applySchemaChange（HTTP 执行 Doris DDL）
                                         → FINISHED
  ← SchemaChangeResultResponse（finished 事件；SchemaOperator 阻塞轮询中）
  → 下发 finished DDL 到 sink writer（更新本地 schema view）
  数据流恢复
```

**为什么数据会在 DDL 前被阻塞**：`SchemaOperator.processElement` 收到 schema-change 事件后，先向 coordinator
请求 `SchemaChangeRequest`，在拿到 ACCEPTED 响应前，**后续数据事件都停在算子内不下发**。coordinator 则在
`WAITING_FOR_FLUSH` 里攒齐所有并行度的 `FlushSuccessEvent`，确认旧结构数据全部落盘，才执行 DDL 并放行。

**状态机**：`IDLE → WAITING_FOR_FLUSH → APPLYING → FINISHED`，`BUSY` 时新请求排队。这一步和 released 完全一致
（见 coordinator-diff/05）。

---

## 3. 五个自写 coordinator 文件

位于 `sink/schema/coordinator/`，与 released 逐文件对照见 `../coordinator-diff/`（5 份 `.diff`）。

| 文件 | 职责 | 与 released 关键差异 |
|---|---|---|
| `KafkaJsonSchemaRegistry` | 单线程事件循环，持有 manager + derivation + request handler；`checkpointCoordinator` 序列化 manager | **checkpoint 不序列化 derivation**（derivation 无状态，见 §4）；`resetToCheckpoint` 只留 version-2 分支 |
| `KafkaJsonSchemaRegistryRequestHandler` | 状态机、flush 收集、busy 排队 | `applySchemaChange` 用 `instanceof CreateTableEvent` 替代 `getType() != CREATE_TABLE`，**去掉 `acceptsSchemaEvolutionType` 检查**；删除整个 `lenientizeSchemaChangeEvent` |
| `KafkaJsonSchemaManager` | original / evolved 两张 `TableId → SortedMap<Integer, Schema>` 图（各留 3 版） | `instanceof` 分派**全部 10 个事件**；RenameTable 注册新表 + **保留旧表条目**（§5）；DropTable/TruncateTable 不动 schema |
| `KafkaJsonSchemaDerivation` | 路由/派生层 | **削成纯透传**（§4）：只留 `applySchemaChange` 原样返回 |
| `KafkaJsonSchemaRegistryProvider` | `OperatorCoordinator.Provider` | 删 routes 参数；线程名 `"kafka-json-schema-evolution-coordinator"` |

`KafkaJsonSchemaOperatorFactory`（`SimpleOperatorFactory<Event> implements CoordinatedOperatorFactory<Event>`）
把 released `SchemaOperator` + 自写 Provider 组装起来。

---

## 4. 派生层为什么是"无状态透传"

released 的 `SchemaDerivation` 持有一条 `routes`（表合并路由），`applySchemaChange` 遍历 routes，命中后
用 `ChangeEventUtils.recreateSchemaChangeEvent` **重写事件**。这个工具函数内部 `instanceof` 只认 5 个标准事件，
自定义事件落到 `throw new UnsupportedOperationException`。

连接器**不做路由**（不配置 route rules、不做表合并），所以：

- `KafkaJsonSchemaRegistry` 构造不传 routes；
- `KafkaJsonSchemaDerivation.applySchemaChange` 直接 `Collections.singletonList(event)` 原样返回；
- checkpoint 里没有 derivationMapping 可写——**derivation 无状态，无需序列化**。

released 需要持久化 `derivationMapping`（routed table → 原始 tables 集合）是因为路由跨 failover 要恢复；
自写版路由恒为空，序列化它没有意义。这就是 coordinator-diff/03 里
「`// Serialize SchemaDerivation mapping` 两行被替换为注释」的原因。

> **澄清一个常见误区**：不是 `KafkaJsonSchemaManager` 不调用 `serializeDerivationMapping`——released 的
> `SchemaManager` 本来也不调用它。那是 `SchemaRegistry`（coordinator 主类）在 checkpoint 里直接调
> `SchemaDerivation` 的 static 方法。自写版是**整个 derivation 都是空的**，所以 coordinator 侧没有调用。
> 详细见 `../coordinator-diff/README.md`「为什么 KafkaJsonSchemaManager 不调用 serializeDerivationMapping」。

---

## 5. 坑：为什么 RenameTable / DropTable 要保留旧 schema 条目

released `SchemaOperator.processSchemaChangeEvents` 在事件处理**完**之后，会按**旧 tableId** 刷新自身 schema
缓存（`evolvedSchema.put(tableId, getLatestEvolvedSchema(tableId))`）。如果自写 manager 在 rename 时**删掉**了旧
tableId 的条目：

1. SchemaOperator 用旧 tableId 查 `getLatestEvolvedSchema` → **empty**；
2. `put` 一个 empty schema → 后续按旧 tableId 取 schema 的地方抛 `IllegalStateException` → **作业失败**。

所以：
- **RenameTable**：注册新 tableId 的 schema，但**保留旧 tableId 条目**（rename 后不再有旧表名数据，旧条目无副作用）；
- **DropTable**：同样保留条目，DDL 用 `DROP TABLE IF EXISTS` 保证幂等，`isOriginalSchemaChangeEventRedundant` 对 DropTable 恒 false（drop 可重放）。

**FlushEvent 的 tableId 用旧表 id**：flush 冲刷的是 rename DDL **之前**写到旧表名的数据，整个阻塞协议
（request → flush → flushSuccess 校验 → APPLYING）统一用 `RenameTableEvent.tableId()`（旧表 id）。

---

## 6. 事件流全景

```
KafkaJsonDataSource（source）
  └─ transform("kafka-json-schema-operator", KafkaJsonSchemaOperatorFactory)
       │   released SchemaOperator（EVOLVE）：instanceof 分派全部事件；阻塞协调（§2）
       └─ 自写分区链（type 全程连接器类型，自定义事件安全）
            │   SchemaChangeEvent/FlushEvent → 复制到每个分区（KafkaJsonPartitioningEvent{target=i}）
            │   DataChangeEvent → hash(tableId+pk) % 并行度
            └─ transform("kafka-json-sink-writer", new DataSinkWriterOperatorFactory<>(kafkaJsonDorisSink, ...))
                 │   released DataSinkWriterOperator：FlushEvent→writer.flush()+FlushSuccessEvent；
                 │   SchemaChangeEvent→emitLatestSchema；注册 subtask
                 └─ 非 2PC sink → CommittableMessageTypeInfo.noOutput()，链路终结
```

自写分区只涉及 3 个文件（崩点）：`KafkaJsonPartitioningEventSerializer`（payload 走 `KafkaJsonEventSerializer`）、
`KafkaJsonPartitioningEventTypeInfo`（覆盖 `createSerializer`）、`KafkaJsonPrePartitionOperator`（广播深拷贝用
连接器 serializer）。其余分区类复用 released（事件类型无关）。

---

## 7. 验证

```bash
# coordinator 单测（直接驱动状态机，不碰 test harness）
mvn -q -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test -Dtest='KafkaJsonSchemaRegistryRequestHandlerTest,KafkaJsonSchemaOperatorTest' \
  -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true

# 模拟 sink ITCase（MiniCluster + JDK HttpServer 模拟 Doris）
sg docker -c "mvn -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test-compile surefire:test@integration-tests -Dtest='KafkaJsonDdlBlockingITCase' \
  -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true"
```

断言要点：DDL 事件到达后、所有并行度 flush 确认前数据被阻塞；确认后 DDL 执行；再放行新 schema 数据；
至少一个自定义事件（TruncateTable / RenameTable）走通全链路不抛异常。
