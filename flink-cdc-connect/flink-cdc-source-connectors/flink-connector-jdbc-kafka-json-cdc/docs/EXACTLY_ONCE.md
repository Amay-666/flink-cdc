# Canal Kafka CDC：全量→增量切换的 exactly-once 同步机制

本文档描述 flink-connector-jdbc-kafka-json-cdc 如何基于 flink-cdc-base 的增量快照框架（FLIP-27 +
Watermark Signal Algorithm）在**全量快照 → 增量变更**的切换过程中保证数据不丢、不重，
最终在 Flink checkpoint 语义下达到 exactly-once。

## 1. 术语与数据模型

| 名称 | 含义 | 来源 |
|---|---|---|
| `es` | 事件在 MySQL binlog 中的执行时间（executeTime），数据**落库**的时刻 | canal flatMessage 字段 |
| `ts` | canal 解析并生成该条消息的系统时间，晚于 `es` | canal flatMessage 字段 |
| record timestamp | Kafka 消息的物理时间戳（`log.message.timestamp.type=CreateTime` 时=producer 发送时刻） | Kafka `ConsumerRecord.timestamp()` |
| event time | 水位比较使用的事件时间，由 `scan.message.event-time` 决定（默认 `ES`，可配 `TS`） | 消息内容 |

天然时序关系：`es ≤ ts ≤ record timestamp(发送时刻)`。

`KafkaJsonOffset = (eventTime, partition, offset)`，排序规则 `eventTime → partition → offset`。
其中 `eventTime` 取自**消息内容**里的 `es`/`ts`，**不是** Kafka 记录时间戳 —— 这是本设计最
关键的一点：canal 的事件时间在消息体内，Kafka 的索引（按记录时间戳）只是用来近似定位。

## 2. 同步流程总览

```
快照阶段（并行分片）                     增量阶段（单主 stream）
┌─────────────────────────────┐      ┌──────────────────────────┐
│ snapshot split i:           │      │ stream split:             │
│   LOW  ─(capture 水位)      │      │   起点 = min(所有 split    │
│   JDBC 读分片数据           │      │         的 HIGH)          │
│   HIGH ─(再 capture 水位)   │      │   → 持续消费 Kafka        │
│   反填 (LOW, HIGH]          │      │   → shouldEmit 过滤重叠区  │
│   END ─(finished=HIGH)     │      │                          │
└─────────────────────────────┘      └──────────────────────────┘
```

- 快照按分片（chunk）并行读取，每个分片独立完成「LOW → 读 → HIGH → 反填 → END」。
- 所有分片完成后，创建主 stream split，从此接管增量。
- 数据不丢的保证由两段构成：**反填**负责快照读窗口内产生的增量，**主 stream 起点**负责
  快照窗口之后的所有增量。

## 3. 双水位算法（每个 snapshot split）

1. **LOW**：JDBC 读分片数据**之前**，通过 `KafkaJsonDialect.displayCurrentOffset` 捕获当前
   Kafka 流位置（每个分区读最新一条消息得到 `(eventTime, partition, offset)`）。
2. **快照读**：`KafkaJsonScanFetchTask.KafkaJsonSnapshotSplitReadTask` 按分片 SQL 读取 MySQL 行，
   转成 `READ` 记录入队。
3. **HIGH**：快照读**之后**再次捕获流位置。
4. **反填** `(LOW, HIGH]`：用同一个 Kafka consumer 从 LOW 读到 HIGH（含端点），把窗口内
   canal 已写入的变更回放。回放记录与快照记录**按主键**在输出缓冲中覆盖
   （`IncrementalSourceScanFetcher.rewriteOutputBuffer`），保证「先快照、后反填」重叠区
   的最终值正确。
5. **END**：分片收尾，该分片 **finished offset = HIGH**（`IncrementalSourceRecordEmitter` 对
   HIGH 事件 `setHighWatermark`）。

## 4. 主 stream 的起点

主 consumer 从**哪里**开始消费，取决于 startup mode：

| startup mode | assigner | 主 stream 起点 |
|---|---|---|
| `initial`（默认） | `HybridSplitAssigner`（先快照再增量） | **minOffset = 所有已 finished snapshot split 的 HIGH 的最小值**；null 时回退 `createInitialOffset()` |
| `snapshot` | `HybridSplitAssigner` | 同上，但 `stoppingOffset = maxOffset`（有界，读到即停） |
| `earliest` / `latest` / `timestamp` / `specific-offsets` | `StreamSplitAssigner`（纯增量，**无快照**） | 由各自 mode 决定（latest 即时捕获、earliest 从头等） |

> 注意：`scan.kafka.startup.mode` 只影响 `eventTime == 0`（无快照的 stream-only 场景）时的
> 兜底 seek，**不决定**默认 `initial` 下主 stream 的起点。

`minOffset` 是**跨分区**的最小值：`KafkaJsonOffset` 按 `eventTime → partition → offset` 排序取
最小。它与某些分片 HIGH 之间的重叠区间 `(minOffset, HIGH_i]` 会被主 stream 重新读到，由
`IncrementalSourceStreamFetcher.shouldEmit` 按「已完成分片的 highWatermark」过滤掉，
从而**每个键在管线内只发射一次**。

## 5. seek 机制：指定 offset，而非从头读

主 stream 启动时 `KafkaJsonStreamFetchTask.assignAndSeek`：

```
if (eventTime > 0)  →  consumer.offsetsForTimes({每个 partition → eventTime})
                       → 对每个分区 seek 到返回的 offset
                       分区无 ≥eventTime 的消息 → seekToEnd
else                →  按 scan.kafka.startup.mode: EARLIEST→seekToBeginning / LATEST→seekToEnd
```

**核心**：`offsetsForTimes` 是 Kafka 的内置按时间戳定位能力 —— Kafka 按记录时间戳建索引，
传入 `eventTime` 直接返回「第一个记录时间戳 ≥ eventTime 的 offset」。因此**不需要**维护
「kafka offset ↔ 数据 es」的映射，Kafka 自己查询。

## 6. 数据不丢的充分条件

设起始水位为 `T`（即起始 `KafkaJsonOffset.eventTime`）。定位点 `L = offsetsForTimes(T)` 之前的
所有消息，其记录时间戳都 `< T`。**丢数据的唯一来源**是：存在一条消息 `m`，内容事件时间
`eventTime_m ≥ T`（按水位比较应被消费），但记录时间戳 `< T`（被定位点跳过）。

因此不丢的充分条件是：对任意 `m`，**记录时间戳 ≥ eventTime_m**。因为

```
eventTime_m ≥ T  ⟹  记录时间戳_m ≥ eventTime_m ≥ T
```

`m` 必然落在定位点**或之后**，绝不丢失。

由于 `es ≤ ts ≤ 发送时刻 = 记录时间戳`，canal 的发送时序天然满足该条件（无论 eventTime
取 `es` 还是 `ts`）。即使 `log.message.timestamp.type=LogAppendTime`（broker 接收时刻打
时间戳），只会更晚，依然安全。

### 唯一前提：时钟一致

不等式中的 `es` 由 **MySQL 服务器时钟**打点，记录时间戳由 **Kafka broker 时钟**打点。
若两机时钟漂移达到或超过 canal 的同步延迟量级（正常为毫秒级），不等式可能被破坏：

> 例：MySQL 时钟快 1 小时 → `es=10:00` 的消息在 Kafka 时钟 9:00 就已写入 → 记录时间戳
> `9:00 < es` → `offsetsForTimes({partition→10:00})` 跳过它 → **丢失**。

因此部署要求 **MySQL 与 Kafka 集群时间同步（NTP）**。另外要求同一 topic 内 `es` 单调
递增（canal 按 binlog 顺序写入，天然满足），否则水位比较失效。

## 7. exactly-once 论证

| 保证点 | 机制 | 出处 |
|---|---|---|
| 快照分片断点续传 | 每个 snapshot split 的进度进入 Flink checkpoint（split state），重启从断点恢复 | base `IncrementalSourceReader` |
| 增量 offset 与 checkpoint 对齐 | `KafkaJsonDialect.notifyCheckpointComplete` → `KafkaJsonStreamFetchTask.commitCurrentOffset`，仅在 checkpoint 完成后提交 | `KafkaJsonDialect.java` |
| 全量/增量重叠区一致 | 反填记录按主键覆盖快照记录（`rewriteOutputBuffer`） | base scan fetcher |
| 重叠区去重 | 主 stream `shouldEmit` 按已完成分片 HIGH 过滤 | base stream fetcher |
| 源可回放 | Kafka 消息保留 + `offsetsForTimes` 精确 seek | §5 |

Flink checkpoint 语义下：每个记录恰好在一次 checkpoint 边界内被处理，重启后从已提交的
checkpoint（split 进度 + stream offset）恢复，**不重放已提交部分**。两次 checkpoint 之间
失败会重放该窗口内的记录（at-least-once 子集），由 Flink 的 checkpoint 对齐保证管线内部
一致。

**语义边界**：上述 exactly-once 是「source → Flink 管线内部」的保证。端到端 exactly-once
仍需下游 sink 具备幂等性或两阶段提交。

## 8. 相关配置

| 配置 | 作用 |
|---|---|
| `scan.message.event-time` | 水位事件时间来源：`ES`（默认）/ `TS` |
| `scan.kafka.startup.mode` | 无快照（stream-only）时的兜底 seek：`earliest`（默认）/ `latest` |
| `scan.startup.mode` | base 框架启动模式：`initial`（默认）/ `snapshot` / `latest-offset` 等 |
