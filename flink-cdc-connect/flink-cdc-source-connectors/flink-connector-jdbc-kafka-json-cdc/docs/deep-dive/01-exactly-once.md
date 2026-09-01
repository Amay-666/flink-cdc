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

# 全量→增量切换的 exactly-once 机制

> 本文回答一个问题：**连接器从「全量读」切到「增量读」的那一瞬间，会不会把同一条数据发两遍，或者漏掉一条？**
> 一句话答案：**正常情况下不会；边界上曾经有过两个真实的重复缺陷，都已修好并用测试锁死。**
> 关联：[ARCHITECTURE.md](../ARCHITECTURE.md)（总览）、[ROADMAP.md](../ROADMAP.md)（P0 修复记录）。

---

## 1. 术语与数据模型

| 名称 | 含义 |
|---|---|
| `es` | 事件在 MySQL binlog 中的执行时间（executeTime），数据**落库**的时刻（canal flatMessage 字段） |
| `ts` | canal 解析并生成该条消息的系统时间，晚于 `es` |
| event time | 水位比较使用的事件时间，由 `scan.message.event-time` 决定（默认 `ES`，可配 `TS` / `TIDB_TSO`） |

天然时序：`es ≤ ts ≤ 消息写入 Kafka 的时刻`。

`KafkaJsonOffset = (eventTime, partition, offset)`，排序规则 `eventTime → partition → offset`。
**关键点**：`eventTime` 取自**消息内容**里的 `es`/`ts`，**不是** Kafka 记录时间戳——canal 的事件时间在
消息体内，Kafka 的索引（按记录时间戳）只是用来近似定位。

---

## 2. 数据不丢的充分条件

设起始水位为 `T`。定位点 `offsetsForTimes(T)` 之前的消息，其记录时间戳都 `< T`。**丢数据的唯一来源**是：
存在一条消息 `m`，内容事件时间 `eventTime_m ≥ T`（应被消费），但记录时间戳 `< T`（被定位点跳过）。

因此不丢的充分条件是：对任意 `m`，**记录时间戳 ≥ eventTime_m**。因为

```
eventTime_m ≥ T  ⟹  记录时间戳_m ≥ eventTime_m ≥ T
```

`m` 必然落在定位点**或之后**，绝不丢失。

由于 `es ≤ ts ≤ 发送时刻`，canal 的发送时序天然满足该条件。即使 `log.message.timestamp.type=LogAppendTime`
（broker 接收时刻打时间戳），只会更晚，依然安全。

### 唯一前提：时钟一致

`es` 由 **MySQL 服务器时钟**打点，记录时间戳由 **Kafka broker 时钟**打点。若两机时钟漂移达到或超过
canal 的同步延迟量级（正常为毫秒级），不等式可能被破坏：

> 例：MySQL 时钟快 1 小时 → `es=10:00` 的消息在 Kafka 时钟 9:00 就已写入 → 记录时间戳 `9:00 < es`
> → `offsetsForTimes` 跳过它 → **丢失**。

因此部署要求 **MySQL 与 Kafka 集群时间同步（NTP）**；且同一 topic 内 `es` 单调递增（canal 按 binlog
顺序写入，天然满足）。

---

## 3. 双水位算法（每个快照分片）

每个 snapshot split（分片）独立完成「读一小块 → 记录边界 → 补读边界内变化」：

1. **LOW**：JDBC 读分片数据**之前**，捕获当前 Kafka 流位置（`KafkaJsonDialect.displayCurrentOffset`）。
2. **快照读**：按分片 SQL 读取 MySQL 行，转成 `READ` 记录入队。
3. **HIGH**：快照读**之后**再次捕获流位置。
4. **反填** `(LOW, HIGH]`：用同一个 Kafka consumer 从 LOW 读到 HIGH（含端点），把窗口内 canal 已写入的
   变更回放。回放记录与快照记录**按主键**在输出缓冲中覆盖（`rewriteOutputBuffer`），保证「先快照、后反填」
   重叠区的最终值正确。
5. **END**：分片收尾，该分片 **finished offset = HIGH**。

---

## 4. 主 stream 的起点与 seek

主 consumer 从**哪里**开始消费，取决于 startup mode。默认 `initial`（先快照再增量）：
`minOffset = 所有已 finished snapshot split 的 HIGH 的最小值`。

`minOffset` 是**跨分区**的最小值，它与某些分片 HIGH 之间的重叠区间 `(minOffset, HIGH_i]` 会被主 stream
重新读到，由 `IncrementalSourceStreamFetcher.shouldEmit` 按「已完成分片的 highWatermark」过滤掉，
从而**每个键在管线内只发射一次**。

seek 用 **Kafka 内置按时间戳定位**：

```
consumer.offsetsForTimes({每个 partition → eventTime})
   → 对每个分区 seek 到「第一个记录时间戳 ≥ eventTime 的 offset」
   分区无 ≥eventTime 的消息 → seekToEnd
```

**核心**：不需要维护「kafka offset ↔ 数据 es」的映射，Kafka 自己查询。`offsetsForTimes` 是 Kafka 的内置能力。

---

## 5. 边界问题：交接点会不会重复？（通俗版）

> 历史上这里有过**两个真实的重复缺陷**。下面用大白话讲清楚，末尾给"懂行的人"结论和代码位置。

### 5.1 问题背景：交接点怎么产生重复

全量不是一眨眼读完的，它要读一小会儿。**就在读的这几秒里，数据库可能又变了。** 这些「读的过程中
新发生的变化」两头都不靠——既不在全量里，又比增量开始得早。

处理办法是 **回填**：全量读完之后，把「读的过程中新发生的变化」再补读一遍。回填 + 增量 = 数据一条不丢。

于是交接点上，同一条变化可能有两个来源：**回填发一遍，增量再发一遍 → 下游收到两遍（重复）。**
我们要防的，就是这个重复。

### 5.2 曾经担心但实际不会重复的：边界消息恰好等于分界线

全量读到哪一刻为止会记一个分界线（**HIGH**）。假如有一条消息，它的发生时间**恰好等于分界线**——这一条，
回填会发一次，增量会不会**再发一次**？

**结论：不会。** 原因是消息排序上有个很巧的细节：真实消息永远排在「分界线标记」**前面**，哪怕两者时间
相同（`KafkaJsonOffset` 用 `partition=MAX` 做哨兵，真实消息 `partition < MAX`）。所以那条边界消息：

- 回填发一次；
- 增量即使读到它，也会被自己的检查逻辑拦下（它的时间和分界线相同，不算"越过"分界线）。

**只有一个大块（不分块）的时候，本来就完美——不多发，也不漏发。**

### 5.3 真正的问题一：整批超发（有界回填越界）

这是**最早发现并修掉**的重复。有界回填本来应该"读到第一个越过 ending 的消息就停"，但旧实现是
**消费一整批**之后才看"这批有没有越过 ending"。于是一整批里那些 `es ≥ HIGH` 的消息（快照期间新到的变更）
也被回填发出；而流式阶段从 `offsetsForTimes(HIGH)` 开始读，会对这批 es≥HIGH 记录**再放行一次** → 重复。

> 修复：有界读把 ending offset 当**排他**上界——es ≥ ending 的消息一律不 emit（归流式阶段，流式从含端点
> 的起点读，正好补上，无丢失）；终止条件改成**每个**已分配分区都出现 es ≥ ending（或排空到 log end）才收尾，
> 不能见首个越界就停（否则同批其它分区未读的 es<ending 消息会丢）。
> 详见 [ROADMAP.md P0-1](../ROADMAP.md#p0-1-流式边界语义审计与修复-)。

### 5.4 真正的问题二：快车道开关 + 跨队列乱序

增量读有个「快车道开关」：一旦它看到任何一条**比全量覆盖更新的**消息，就认为"从现在起全是新数据"，
**不再逐个检查**，来了就发。

单队列时这样没问题：老消息肯定先到，开关没打开前就被正常检查掉了。但 Kafka 把消息分散在**好几个队列
（分区）**里，**跨队列的顺序不保证**。举个具体例子：

- 队列 A（热闹）：先送来一条新消息 → **快车道开关被打开**；
- 队列 B（冷清）：才慢吞吞送来一条**旧**消息——而这条旧消息，**回填早就发过了**。

开关已经打开 → 这条旧消息不再被检查 → **又被发了一遍 → 重复。**

### 5.5 怎么修：两道闸门

思路一句话：**在增量入口，把「回填已经发过的」再拦一道，让它到不了下游。**

**第一道闸（太老的，一律拦下）**：增量路径上，任何一条消息，只要它的发生时间**不晚于全量覆盖的最早时刻**，
直接扔掉——这些数据回填早就处理过了。不管快车道开关有没有打开、消息乱不乱序，这道闸都成立。

**第二道闸（属于某块、且没超过那块覆盖时间的，拦下）**：一个大表会被切成**几块**分别全量读，每块各自记了
自己的覆盖时刻。此时只看"最早时刻"就不够了——举个例子：

- 第 1 块覆盖到 **3 点**，第 2 块覆盖到 **4 点**；
- 有一条 3 点半的消息，比"最早时刻（3 点）"**新**，第一道闸拦不住；
- 但它在第 2 块的范围里，而第 2 块已经覆盖到 4 点了——**回填已经发过它了。**

所以第二道闸记得**每块的主键范围和它的覆盖时刻**。增量来一条消息，问两件事：这条消息的主键属于哪一块？
它的发生时间还在那一块的覆盖时刻之内吗？两个都是 → 回填已经发过 → 扔掉；否则 → 新数据 → 放行。

**真正的新数据**（发生时间超过自己那一块的覆盖时刻）**永远放行**，不受影响。

### 5.6 问题三：min 水位漏掉"读库期间提交的变更"

用户实测复现的一个真实重复：快照阶段写入一条 UPDATE，之后程序（JDBC 快照读）查询到它——快照行已含该变更的
效果；这条 canal 日志在 Kafka 里被程序**完成快照阶段后再次捕获发到下游** → 重复。

**根因**：`queryCurrentOffset` 返回各 partition 最新消息 es 的**最小值** `(min, -1, -1)` 作 HIGH 水位。
读库期间提交的变更 E 若 `min < E ≤ 真实边界`：
- 有界回填以 `es ≥ HIGH`（= min）为排他上界 → 把 E 剔除，不覆盖；
- 流式 `shouldEmit` 的 `isAfter((min,-1,-1))` → 把 E 再放行；
- 而 JDBC 快照已读到 E 的效果 → 下游收到「快照行 + 同一变更」= **重复**。

**修复**：`queryCurrentOffset` 改为各 partition 最新消息 es 的**最大值** + sentinel 分区/偏移
`(max, Integer.MAX_VALUE, Long.MAX_VALUE)`。真实记录分区恒 < MAX，故 es==max 的边界记录排在水位**之前**
（`isAfter` 为 false）→ 归属回填。同时有界读新增下界 `es < starting` 一律丢弃（快照前的变更若覆盖快照行，
可能把其已被后续变更 supersede 的值回退回去）。
语义变为：单 split 回填发 `[LOW, HIGH]`（含端点），流式发 `(HIGH, …)`；读库期间提交的任意变更都落在
`(low, high]` 窗口内，由回填覆盖/新增/删除恰一次。

### 5.7 天然安全的场景：TiDB 用数据库时间戳做边界

当数据库是 TiDB、用数据库的提交时间戳（TSO）做边界时，边界时刻是「我们查询的那一瞬间」，而所有消息的
提交时间都**严格早于**这一瞬间——**不存在恰好等于边界时刻的消息**。所以这个场景从原理上就不可能重复。

### 5.8 其它顺带发现（不是 bug）

- **一段没人用的死代码**：`KafkaJsonOffsetSupplier` / `getOffsetSupplier()` 无生产调用。留着下次清理。
- **文档过时**：有两份文档写的窗口区间和实际代码对不上。不是 bug，已随本次文档整理订正。

---

## 6. exactly-once 论证总表

| 保证点 | 机制 | 出处 |
|---|---|---|
| 快照分片断点续传 | 每个 snapshot split 的进度进入 Flink checkpoint（split state），重启从断点恢复 | base `IncrementalSourceReader` |
| 增量 offset 与 checkpoint 对齐 | `KafkaJsonDialect.notifyCheckpointComplete` → `KafkaJsonStreamFetchTask.commitCurrentOffset`，仅在 checkpoint 完成后提交 | `KafkaJsonDialect.java` |
| 全量/增量重叠区一致 | 反填记录按主键覆盖快照记录（`rewriteOutputBuffer`） | base scan fetcher |
| 重叠区去重 | 主 stream `shouldEmit` 按已完成分片 HIGH 过滤 | base stream fetcher |
| 源可回放 | Kafka 消息保留 + `offsetsForTimes` 精确 seek | §4 |

Flink checkpoint 语义下：每个记录恰好在一次 checkpoint 边界内被处理，重启后从已提交的 checkpoint
（split 进度 + stream offset）恢复，**不重放已提交部分**。两次 checkpoint 之间失败会重放该窗口内的记录
（at-least-once 子集），由 Flink 的 checkpoint 对齐保证管线内部一致。

**语义边界**：上述 exactly-once 是「source → Flink 管线内部」的保证。端到端 exactly-once 仍需下游 sink
具备幂等性或两阶段提交。

---

## 7. 给懂行的人（结论 + 代码位置）

### 7.1 三个重复缺陷的结论对照

| 问题 | 结论 | 一句话解释 |
|---|---|---|
| 边界消息恰好等于分界线 | **证伪（不会重复）** | 水位哨兵 `(es, MAX, MAX)` 的 `partition=MAX` 使真实消息同 es 时排在水位**之前** → `isAtOrAfter(HIGH)` 对 `es == HIGH` 为 false → 边界消息被 per-split 分支正常丢弃，单 split 单分区本就 exactly-once |
| 整批超发（有界回填越界） | **确认，已修** | 有界回填旧实现消费整批后才判越界；修复为 ending 排他上界 + 每分区都越界才收尾（`KafkaJsonStreamFetchTask`） |
| 快车道开关 + 跨队列乱序 | **确认，已修（两道闸）** | `IncrementalSourceStreamFetcher.shouldEmit`（L178-203）在 `hasEnterPureStreamPhase`（L205-225）触发后对该表**全部记录短路放行**；多分区 poll 乱序下已回填记录（`es ≤ 所属 HIGH`）在触发后被二次发出 |
| min 水位漏掉读库期间变更 | **确认，已修** | `queryCurrentOffset` 改 max + sentinel；有界读 `[LOW, HIGH]` 含端点 |
| TiDB 用数据库时间戳做边界 | **确认免疫** | TiDB+es 边界为 TSO，无消息 `es == TSO`，流式 `isAfter(TSO)` 天然全过滤 |

### 7.2 修复与测试代码位置

| 内容 | 位置 |
|---|---|
| 第一道闸排他下界（`es ≤ startingOffset` 丢弃） | `KafkaJsonStreamFetchTask.java` L236-250 |
| 第二道闸多块预过滤（`isCoveredByFinishedSnapshotSplit`） | `KafkaJsonStreamFetchTask.java` L287-299（调用）、L330-349（实现） |
| 第二道闸数据源（finished split 信息装配） | `HybridSplitAssigner.createStreamSplit` L228-276、`IncrementalSourceReader.fillMetaDataForStreamSplit` L417-463 |
| `isRecordBetween`（第二道闸复用） | 基座 `JdbcSourceFetchTaskContext` L72-76 |
| 快车道短路放行根因 | 基座 `IncrementalSourceStreamFetcher.shouldEmit` L178-203 / `hasEnterPureStreamPhase` L205-225 |
| 哨兵排序 | `KafkaJsonOffset`（真实消息 `partition < MAX`） |
| 边界消息不重复实证 | 真实 `KafkaJsonOffset` 的 OrderProbe：`(3000,0,0).isAtOrAfter((3000,MAX,MAX)) = false` |
| 回归测试 | `KafkaJsonStreamFetchTaskTest`：边界消息丢弃、被早期分片覆盖的记录丢弃、min→max 水位 |

> 历史说明：边界问题初稿曾把「快车道 + 乱序」记为"理论、待证伪"，且多算了一个"源级重复"风险；逐行核对
> 基座 scan fetcher 的 chunk 过滤后排除后者，确认它是**唯一**真实存在的边界缺陷（2026-08-14 修正）。
