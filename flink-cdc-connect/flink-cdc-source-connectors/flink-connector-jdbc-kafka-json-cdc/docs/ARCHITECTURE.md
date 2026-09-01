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

# jdbc-kafka-json-cdc：架构总览

> 分支：`feature/canal-rename-plan-a`（基于 `feature/3.2.1-custom`，flink-cdc 3.2.1）
> 状态：持续更新中（Phase 13 Doris sink / 自写 coordinator 进行中）
> 本文档是**总览**，只讲"是什么、数据怎么流、关键决策"；实现原理与代码解读在 [deep-dive/](./README.md) 子文档里，按需阅读。

---

## 1. 这个连接器是什么

一个 **Kafka → 下游** 的 CDC 连接器：外部工具（canal / Debezium）把数据库的增量与 DDL 写入 Kafka，
连接器消费这些 **JSON 消息**，结合 **JDBC 全量快照**，产出统一的 **Flink CDC Event 流**。

两个核心能力：

1. **全量 + 增量无缝衔接**：首次同步用 JDBC 分片并行读存量，之后切换为消费 Kafka 增量，靠水位算法保证**不丢、不重**（详见 [deep-dive/01-exactly-once.md](./deep-dive/01-exactly-once.md)）。
2. **DDL 感知**：消费 canal / Debezium 的 DDL 消息，解析成 Flink CDC 的 `SchemaChangeEvent`，让下游能同步 schema 演进。

名字由来：本连接器最初叫 **canal**，但它是"消费 Kafka 上的 JSON 消息"的通用源，于是整体改名为
**jdbc-kafka-json-cdc**。用户可见名与包名解耦：

| 层 | 命名 |
|---|---|
| 用户可见（模块 / factory 标识） | `flink-connector-jdbc-kafka-json-cdc` / `flink-cdc-pipeline-connector-jdbc-kafka-json` / `jdbc-kafka-json-cdc` |
| 开发者（包 / 类前缀） | `org.apache.flink.cdc.connectors.kafkajson` / `KafkaJson*` |
| wire 格式（配置值，**不随改名变化**） | `scan.message.format = canal / debezium`；`scan.database.type = mysql / postgres / tidb` |

---

## 2. 两个模块 + 边界

| 模块 | 路径 | 职责 |
|---|---|---|
| **source 模块** | `flink-connector-jdbc-kafka-json-cdc` | 原始 FLIP-27 `KafkaJsonSource<SourceRecord>`：消费 Kafka、canal 消息桥接、JDBC 快照、**DDL 双解析器**、源侧 schema 状态 |
| **pipeline 模块** | `flink-cdc-pipeline-connector-jdbc-kafka-json` | `KafkaJsonDataSource`（flink-cdc `DataSource`）+ **Event 层**：`KafkaJsonEventDeserializer`、`RenameTableEvent`、自定义序列化栈、自写 sink / coordinator |
| **released 模块** | `flink-cdc-common` / `flink-cdc-runtime` / 其他 connector | 发行版，**绝不可改**（硬约束） |

> **硬约束：不改 released 模块。** 需要新事件（如 `RenameTableEvent`）、新序列化、新协调逻辑时，
> 全部在 pipeline 模块内自建副本。这是贯穿本连接器的设计主线。

---

## 3. 总体数据流

先用一张总图看懂数据怎么从数据库流到 Doris，再拆开看快照/增量两条路径的汇合、released 复用边界、
sink 链的 DDL 阻塞。每个环节的深入原理在对应 deep-dive 子文档里。

### 3.1 全链路总图

```
数据源层（外部，非本连接器）
    MySQL / TiDB / PostgreSQL ──binlog/raft──▶ canal-server / Debezium / TiCDC
        └── 序列化成 JSON（canal flatMessage / Debezium envelope）──▶ Kafka
        │
        │  全量：JDBC 分片直连数据库             增量：消费 Kafka JSON 消息
        ▼                                     ▼
source 模块  flink-connector-jdbc-kafka-json-cdc
    KafkaJsonTableSourceFactory → KafkaJsonSourceBuilder → KafkaJsonSource<T>
        （extends JdbcIncrementalSource：createEnumerator / restoreEnumerator /
         splitReader 继承 released 框架；仅覆写 createReader，KafkaJsonSource.java:84）
    ├─ 快照路径（全量）
    │    KafkaJsonChunkSplitter（JDBC 分片）→ KafkaJsonScanFetchTask（按 chunk 读存量行）
    │        └─ 每行经 KafkaJsonRecordFactory 转成 SourceRecord
    ├─ 增量路径（流式）
    │    KafkaJsonStreamFetchTask（poll Kafka）
    │        └─ KafkaJsonParserFactory → canal / Debezium 子包消息解析器
    │            ├─ 数据消息 → KafkaJsonRecordConverter → SourceRecord
    │            └─ DDL 消息 → KafkaJsonSchemaChangeHandler
    │                  └─ KafkaJsonDdlParser 双实现：Druid（默认）/ Debezium
    │                       共享类型转换层（KafkaJsonValueConverter）
    │  两条路径共用 KafkaJsonRecordFactory，产出 envelope 形状一致的 SourceRecord
    ▼
    KafkaJsonSourceReader（extends IncrementalSourceReaderWithCommit，KafkaJsonSourceReader.java:44）
        · 双水位算法：快照 LOW/HIGH 水位 + 增量 backfill (LOW, HIGH] 回填
        · split 分配 / 切换、emit、checkpoint 完成后提交 Kafka offset
    ▼  SourceRecord 流
pipeline 模块  flink-cdc-pipeline-connector-jdbc-kafka-json
    KafkaJsonDataSource（implements DataSource）
    ├─ getEventSourceProvider() → KafkaJsonEventSource
    │      （extends KafkaJsonSource<Event>，复用 source 模块全链路）
    ├─ getMetadataAccessor()   → KafkaJsonMetadataAccessor
    ├─ KafkaJsonPipelineRecordEmitter（extends IncrementalSourceRecordEmitter）
    │      把 reader 产出的 SourceRecord 交给 deserializer
    └─ KafkaJsonEventDeserializer（extends DebeziumEventDeserializationSchema）
            数据记录 → DataChangeEvent（父类处理）
            CREATE → CreateTableEvent；其余 DDL → 标准 SchemaChangeEvent
            RENAME_TABLE → RenameTableEvent（KafkaJsonEventDeserializer.java:176 重建）
            TRUNCATE → TruncateTableEvent
            事件流类型：KafkaJsonEventTypeInfo（自写序列化栈）
    ▼  Event 流（SchemaChangeEvent / DataChangeEvent / FlushEvent）
sink 链（本连接器自建 DataStream，见 3.4 放大）
    KafkaJsonSchemaOperatorFactory（持 released SchemaOperator，EVOLVE 行为）
        · 阻塞协调：等所有并行度 flush → 执行 DDL → 放行
    ▼
    PrePartition 分区（KafkaJsonPrePartitionOperator + 复用 released 分区类）
        · SchemaChange / Flush 广播到每个并行度；DataChange 按「表 + 主键」hash
    ▼
    DataSinkWriterOperator（复用 released）→ DorisSinkWriter → DorisHttpClient
        · StreamLoad PUT 写数据 / HTTP 执行 DDL（全 HTTP、非 2PC）
```

### 3.2 快照 / 增量两条路径怎么汇合

- **快照阶段**：JDBC 分片（chunk）并行读存量，逐行转成 SourceRecord。
- **增量阶段**：消费 Kafka，canal / Debezium JSON → SourceRecord。
- **两者共用同一个 `KafkaJsonRecordFactory`**，产出形状一致的 SourceRecord，保证全量/增量切换正确。
- 快照/流切换的不丢不重由**双水位算法**保证（[01-exactly-once.md](./deep-dive/01-exactly-once.md)）。

### 3.3 复用边界：released vs 自写

判据一句话：**事件类型无关的类直接复用；与自定义事件耦合的环节全部在 pipeline 模块自建副本。**

```
released 发行版（零改动，硬约束；事件类型无关，直接复用）
├─ flink-cdc-base
│    · JdbcIncrementalSource 骨架 + createEnumerator / restoreEnumerator
│    · IncrementalSourceSplitReader / IncrementalSourceRecordEmitter
│    · IncrementalSourceReaderWithCommit（checkpoint 提交转发）
│    · SourceReaderMetrics / FutureCompletingBlockingQueue
├─ flink-cdc-runtime / flink-cdc-common
│    · SchemaOperator（EVOLVE）+ DataSinkWriterOperatorFactory
│    · SchemaEvolutionClient + 协调 RPC DTO（SchemaChangeRequest / Response、
│      FlushSuccessEvent / SinkWriterRegisterEvent / GetEvolvedSchemaRequest 等）
│    · 分区链复用：PartitioningEvent / EventPartitioner /
│      PartitioningEventKeySelector / PostPartitionProcessor
│    · FlushEvent / DefaultDataChangeEventHashFunctionProvider /
│      DebeziumEventDeserializationSchema（pipeline deserializer 的父类）
        │ 继承 / 直接复用
        ▼
本连接器（自建副本，两个模块）
├─ source 模块  flink-connector-jdbc-kafka-json-cdc
│    · KafkaJsonSource / KafkaJsonSourceReader / KafkaJsonDialect / KafkaJsonTiDBDialect
│    · KafkaJsonChunkSplitter / KafkaJsonScanFetchTask / KafkaJsonStreamFetchTask
│    · 消息解析子包（KafkaJsonMessageParser 等）/ KafkaJsonRecordConverter / KafkaJsonRecordFactory
│    · KafkaJsonSchemaChangeHandler / KafkaJsonDdlParser 双实现 / 源侧 schema / KafkaJsonOffset
├─ pipeline 模块  flink-cdc-pipeline-connector-jdbc-kafka-json
│    · 事件与序列化：5 个自定义事件 + KafkaJsonEventSerializer / KafkaJsonEventTypeInfo /
│      KafkaJsonSchemaChangeEventSerializer（instanceof 分派 5 标准 + 5 自定义）
│    · 自写 coordinator：KafkaJsonSchemaRegistry / RegistryRequestHandler /
│      SchemaManager / SchemaDerivation / RegistryProvider
│    · 自写分区崩点：KafkaJsonPrePartitionOperator /
│      KafkaJsonPartitioningEventTypeInfo + KafkaJsonPartitioningEventSerializer
│    · Doris sink（全 HTTP）：DorisSink（非 2PC）/ DorisSinkWriter / DorisMetadataApplier /
│      DorisDdlBuilder / DorisHttpClient / DorisRowConverter / DorisDataSinkOptions
```

### 3.4 sink 链：DDL 阻塞协调放大

自建 DataStream 组装在 pipeline 模块 `sink/KafkaJsonDataSinkBuilder.java`
（`:113` schema 算子 → `:135` 分区 → `:155` writer）。跨进程的**阻塞-确认协议**
见 [04-ddl-blocking.md](./deep-dive/04-ddl-blocking.md)：

```
每并行度的 sink 算子                              JobManager：KafkaJsonSchemaRegistry
（SchemaOperator + DataSinkWriterOperator）        （单线程事件循环）

      │  收到 SchemaChangeEvent（标准或自定义）
      │  ① SchemaChangeRequest ──▶
      │      coordinator：IDLE → 去重/派生 → WAITING_FOR_FLUSH，回 ACCEPTED
      │  ② 下发 FlushEvent（经分区链广播到全部并行度）
      ▼
    DataSinkWriterOperator：FlushEvent → writer.flush()
      → DorisSinkWriter 强制 StreamLoad PUT（数据已提交）
      → 上报 FlushSuccessEvent
      │  ③ FlushSuccessEvent ──▶
      │      coordinator：WAITING_FOR_FLUSH → 收齐全部并行度的确认 → APPLYING
      │        · DorisMetadataApplier.applySchemaChange
      │        ·   └ DorisDdlBuilder（事件 → Doris SQL）
      │        ·   └ DorisHttpClient.executeSql（HTTP DDL）
      │        · → FINISHED
      │  ④ SchemaChangeResultResponse（finished）◀──
      ▼
    SchemaOperator 收到 finished → 放行数据流，新 schema 生效
```

> 协调器内部对 5 标准 + 5 自定义事件全部用 `instanceof` 分派，不碰 released 的
> `getType()` / `acceptsSchemaEvolutionType`——这是必须自写 coordinator 的根本原因（[04](./deep-dive/04-ddl-blocking.md) §1）。

---

## 4. 关键设计决策

| 决策 | 内容 | 详见 |
|---|---|---|
| **统一载体** | canal / Debezium 消息与 JDBC 快照行统一转成 **debezium envelope 形状的 SourceRecord**，下游 deserializer 统一处理 | [02](./deep-dive/02-message-parsing.md) |
| **自包含序列化栈** | 新事件（RenameTable 等）的序列化全部复制到 pipeline 模块，released 零改动 | [03](./deep-dive/03-event-model.md) |
| **DDL 双解析器** | `scan.ddl.parser = druid`（默认，Alibaba）或 `debezium`（ANTLR），共享类型转换层 | [02](./deep-dive/02-message-parsing.md) |
| **自写 coordinator** | pipeline 侧自建 `KafkaJsonSchema*` coordinator，实现 DDL 阻塞-刷新-执行-放行，自定义事件安全 | [04](./deep-dive/04-ddl-blocking.md) |
| **Doris 全 HTTP** | 不引入 doris connector jar：StreamLoad 写入 + HTTP 执行 DDL，OkHttp 3.14.9 | [05](./deep-dive/05-doris-sink.md) |
| **验证基建** | Workload / Ledger / 对账 / 审计日志，属性级黑盒校验 | [06](./deep-dive/06-verification-infra.md) |

---

## 5. 已知边界（一句话版）

细节见各子文档，这里只列最重要的三条：

1. **`RenameTableEvent.getType()` 返回占位值 `CREATE_TABLE`**——released 枚举里没有 RENAME_TABLE。
   自写序列化栈按 `instanceof`/class 分派不受影响，但**别把它喂给 released 的 SchemaManager/SchemaDerivation/EventSerializer**。
2. **快照阶段 `displayCurrentOffset` 的边界来源**：MySQL 强制跳过有界回填（topic 无持续边界信号），
   TiDB 用 `TIDB_WATERMARK` 推进位移、默认开启回填。
3. **增量阶段的 DDL 只会对"流中见过 CREATE"的表产出列级事件**（L2 注册表约束）。

---

## 6. 文档导航

需要深入了解时，按主题进入：

- **全量→增量切换与边界正确性** → [deep-dive/01-exactly-once.md](./deep-dive/01-exactly-once.md)
- **消息解析（canal / Debezium / DDL）** → [deep-dive/02-message-parsing.md](./deep-dive/02-message-parsing.md)
- **事件模型与序列化栈（含 RENAME Plan A）** → [deep-dive/03-event-model.md](./deep-dive/03-event-model.md)
- **DDL 阻塞协调机制（自写 coordinator）** → [deep-dive/04-ddl-blocking.md](./deep-dive/04-ddl-blocking.md)
- **Doris 写入与 DDL 执行** → [deep-dive/05-doris-sink.md](./deep-dive/05-doris-sink.md)
- **正确性验证基建** → [deep-dive/06-verification-infra.md](./deep-dive/06-verification-infra.md)
- **待办与优先级** → [ROADMAP.md](./ROADMAP.md)
- **开发注意事项 / 坑 / 扩展指南** → [DEV_NOTES.md](./DEV_NOTES.md)
