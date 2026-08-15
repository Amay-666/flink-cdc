# jdbc-kafka-json-cdc 正确性校验方案（真实数据 + 可观测性）

> 本文档描述对 `flink-connector-jdbc-kafka-json-cdc` 做**正确性校验**的完整方案：用**真实数据**
> （真实 MySQL + canal-server / 真实 TiDB + TiCDC 写入 Kafka 的消息）驱动被测连接器，把正确性
> 定义成**可观测的属性**（下游收敛、无重无丢、值/类型保真），用独立于被测件的 ground-truth 账本
> + 对账 + 指标来判定，而不是断言"固定场景的精确事件序列"。
>
> 配套文档：[ARCHITECTURE.md](./ARCHITECTURE.md)（架构与数据流）、[EXACTLY_ONCE.md](./EXACTLY_ONCE.md)
> （快照→增量切换的 exactly-once 机制）、[BOUNDARY_AUDIT.md](./BOUNDARY_AUDIT.md)（边界问题审计）、
> [ROADMAP.md](./ROADMAP.md)（优先级与历史修复）。
>
> 状态：2026-08-15 初稿（设计阶段，尚未动手实现）。分支：`feature/canal-rename-plan-a`。

---

## 目录

1. [现状盘点与缺口](#1-现状盘点与缺口)
2. [校验目标：5 条可观测的正确性属性](#2-校验目标5-条可观测的正确性属性)
3. [总体架构：三件套](#3-总体架构三件套)
4. [真实数据层：Workload 引擎 + Ground-truth 账本](#4-真实数据层workload-引擎--ground-truth-账本)
5. [可观测性层：审计日志 / 指标 / 不变量 / 对账报告](#5-可观测性层审计日志--指标--不变量--对账报告)
6. [对账校验层：Mode A（事件级）+ Mode B（状态级）](#6-对账校验层mode-a事件级--mode-b状态级)
7. [韧性验证：恢复可观测](#7-韧性验证恢复可观测)
8. [场景矩阵：确定性 + 随机化两层](#8-场景矩阵确定性--随机化两层)
9. [验收标准与质量门槛](#9-验收标准与质量门槛)
10. [需新建的测试基建](#10-需新建的测试基建)
11. [落地顺序（分阶段）](#11-落地顺序分阶段)
12. [风险与限制](#12-风险与限制)

---

## 1. 现状盘点与缺口

### 1.1 已有测试基线

| 层 | 内容 | 覆盖 |
|---|---|---|
| **单测** | source 模块 ~180 例 + pipeline 模块 ~20 例 | parser / converter / offset / DDL 双解析器 / 类型转换 / 边界回归（F1 / F4 Tier1 / Tier2） |
| **Phase-11 ITCase** | `MySqlCanalChainITCase`（真 MySQL + canal-server v1.1.8）、`TiDBCdcChainITCase`（真 TiDB + TiCDC）、`MySqlCanalSimulatedChainITCase`、`TiDBSimulatedChainITCase`（simulated 基线） | 快照 4 行 + 3 条 DML + 1 条 ALTER，**精确序列断言** |
| **手动工具** | `MySqlCanalManualHarness`（`main()`） | 实时观察事件流 |

### 1.2 三个结构性缺口（本方案要补的）

1. **断言的是"精确事件序列"，不是"下游状态收敛"。** `containsExactly(...)` 只证明一条固定路径
   对；遇到 TiCDC `old` 全行/半行这类合法 wire 差异就会脆断。用户真正关心的属性——**镜像表最终
   等于源表、无丢无重、值类型保真**——现有测试没有直接断言。
2. **真实数据但量小且固定。** 现有 ITCase 注释明确写了"DML 只在快照阶段结束后执行"，所以
   快照期间写入（ROADMAP P0-4 的重复根因）、边界时刻写入（BOUNDARY_AUDIT F1/F4）、大表多 chunk、
   全类型列、DDL storm、RENAME TABLE（Plan A 头条特性）这些**高危窗口完全没有真实数据覆盖**。
3. **没有观测面。** 无指标、无审计日志、无在线不变量检查。连接器输出的 `Event` 不带
   `es/ts/partition/offset` 元数据——发射时 `KafkaJsonPipelineRecordEmitter.processElement` 手里有
   `SourceRecord`（含 offset 与 `source.ts_ms`），但转成 `Event` 时丢掉了。跑一小时之后无法回答
   "到底对不对、差在哪"。

---

## 2. 校验目标：5 条可观测的正确性属性

每条属性都绑定一个**外部可观测信号** + **判定方法**（不窥探被测件内部状态）：

| # | 属性 | 可观测信号 | 判定方法 |
|---|---|---|---|
| P1 | **状态收敛** | 镜像表 vs 源表逐主键行指纹 diff | `diff 集合为空` |
| P2 | **无丢无重（exactly-once）** | ground-truth 账本 vs 事件流逐 key 对应 | 每个 DML 恰好映射一次；无孤儿事件；无重复 key-insert |
| P3 | **值/类型保真** | 行指纹（规范化序列化，含类型 / NULL / 精度） | 逐列一致 |
| P4 | **边界/切换正确** | 快照→增量切换窗口内无重复 + 最终收敛 | 边界场景矩阵（§8）+ 收敛断言 |
| P5 | **恢复/断点续传正确** | 重启后 lag 归零、重复计数不增、仍收敛 | 韧性场景（§7） |

> 设计取向：**属性级、黑盒、收敛型**断言，而非精确序列断言。即使合法 wire 形状有差异
> （如 canal 半行 `old` vs TiCDC 全行 `old`），只要下游收敛到源库，正确性判定就成立。

---

## 3. 总体架构：三件套

```
   WorkloadDriver（真实 DML/DDL 生成器，独立于被测件）
        │  JDBC 执行           │ 独立维护"期望表"内存模型
        ▼                       ▼
   源库（MySQL / TiDB）   Ground-Truth 账本（Ledger）
        │ binlog → canal-server / TiCDC
        ▼
   Kafka（真实 canal/TiCDC 消息）
        ▼
   连接器 jdbc-kafka-json-cdc（被测，纯黑盒）
        ▼
   [Audit 元数据包装] ──► 审计日志 topic（es/ts/partition/offset/db.table）
        │
        ├──► 不变量校验算子（在线） ──► 违规计数器 + 违规日志（指标）
        │
        ├──► 镜像 Sink（自写，参考 KafkaJsonRenameStateOperator） ──► 镜像库表
        │
        └──────────────► 对账器（源 vs 镜像 / 账本 vs 审计） ──► 差异报告（可观测证据）
```

三个设计要点：

1. **被测件保持黑盒**——只用它对外输出的 `Event` 流和可观测信号判断，不注入探针进连接器内部。
2. **Ground-truth 账本与连接器完全独立**——`WorkloadDriver` 记录"我执行了什么"，不经过连接器，
   构成"应该发生什么"的权威答案。
3. **两条校验路径互补**：
   - **Mode A（事件级对账）**：直接验证连接器输出的事件流本身（P2/P4，无重无丢、边界），
     不依赖任何 sink，最快落地；
   - **Mode B（状态级对账）**：验证"源库 → 镜像表"的端到端结果（P1/P3，收敛、值保真），
     是用户可见的最终正确性。

---

## 4. 真实数据层：Workload 引擎 + Ground-truth 账本

### 4.1 `WorkloadDriver`

测试 util，JUnit 线程，通过 JDBC 以普通客户端身份写源库：

- **有界随机 DML**：INSERT（随机 / 长尾 key）、UPDATE（随机改列）、DELETE；可配置混合比例与
  突发 / 平稳相位。
- **全类型值库**（对应 ROADMAP P1-1）：极值、NULL、`1000-01-01` / `9999-12-31`、DATETIME(6) 微秒、
  DECIMAL(38,18)、科学计数法 DOUBLE、Unicode / emoji、JSON、BIT / ENUM / SET 等。
- **DDL 流**：ALTER ADD / DROP / MODIFY、RENAME COLUMN、RENAME TABLE（Plan A 核心）、按需 CREATE 新表。
- **相位协同**：借助"先读到 N 个快照事件"的信号，把写注入窗口精确压在
  - 快照读期间（复现 ROADMAP P0-4 重复根因）；
  - 边界时刻（复现 BOUNDARY_AUDIT F1/F4）；
  - 快照结束后（平稳增量，作为基线对照）。
- **内存维护"期望表"模型**（每 key 的最终值），**每次操作 append 一条 Ledger**。

### 4.2 `Ledger`（Ground-truth）

- 字段：`seq / op / table / pk / old / new / 执行时刻`。
- 单线程 driver 的执行序 == binlog 序 == canal 写 Kafka 序，所以账本序就是预期事件序。
  对账时**按 key 分组比较**，天然绕开 Kafka 跨分区乱序问题。

> 为什么不用 es 直接对账：driver 拿不到 canal 分配的 binlog `es`。账本用执行序，对账时按 key
> 重建序列即可，不需要 es；**es 只用于边界窗口的深层诊断**（见 §5.1）。

---

## 5. 可观测性层：审计日志 / 指标 / 不变量 / 对账报告

> 这是本方案的核心增量。可观测性既是校验手段（从外部信号判定正确性），也是未来排障的底座。

### 5.1 审计日志（元数据挂载点 + 落盘）

- **挂载点**（已定位，改动只碰 pipeline 模块自己的类）：
  `KafkaJsonPipelineRecordEmitter.processElement(SourceRecord element, SourceOutput<Event> output,
  SourceSplitState splitState)` 在调 `super.processElement` 前，`SourceRecord` 已携带
  `sourceOffset()`（即 `KafkaJsonOffset` 的 es/partition/offset）与 `source.ts_ms`。在此包一层
  `AuditedEvent { Event; long es; long ts; int partition; long offset; TableId }`，随主流旁路写到
  **第二个审计 topic**（或文件）。
- **用途**：可离线重放（把事件流按原样再喂一遍）、grep 定位边界窗口的重复/丢失、喂给 Mode A
  对账器。这是"可观测性"的**数据底座**；不选它，Mode A 和边界诊断都没有素材。
- 约束：不改 `Event` 类本身、不改 released 序列化栈（`AuditedEvent` 是测试侧/旁路概念，
  不进主事件流）。

### 5.2 指标（Metrics）

- **连接器侧**：复用 base 的 `SourceReaderMetrics` + 自加 `Gauge/Counter`——每表每 op 事件计数、
  当前 watermark、快照 split 完成进度、Kafka 消费 lag。
- **验证侧**：不变量违规计数器、对账 diff 计数。
- **出口**：CI 用 MiniCluster 的 metric query service 在测试内直接读断言；可选手动层（§11 V4）
  用 standalone Flink + Prometheus/Grafana 把同一批指标画成看板（事件速率、lag、去重命中、
  违规数），扩展 `MySqlCanalManualHarness` 成"长跑观测台"。

### 5.3 在线不变量校验算子（ProcessFunction）

对事件流做廉价连续检查，违规即 `计数+1` 并写违规日志（不是悄悄错）：

- per-key op 序列合法（delete 后必须有 insert；UPDATE/DELETE 时 key 必须存在）；
- 单分区内 `es` 单调不减（需 §5.1 元数据）；
- 末态与"期望表"逐步比对。

### 5.4 对账报告（可观测证据）

对账器输出结构化 diff：`仅源库的 key` / `仅镜像的 key` / `值差异列` / `类型差异` / `行数` /
`数值列 checksum`。这份报告就是正确性的"可观测证明"，每次跑完落盘留存。

---

## 6. 对账校验层：Mode A（事件级）+ Mode B（状态级）

### 6.1 Mode A · 事件级对账（验证连接器输出本身）

- 输入：审计日志重放 + Ledger。
- 判定：每条 Ledger DML 在事件流中**恰好映射一次**（快照行算 INSERT）；无孤儿事件；按 key
  重建的末态 == 期望表末态。
- 直接命中 P2/P4（无重无丢、边界），**不依赖任何 sink**，最快落地。

### 6.2 Mode B · 状态级对账（验证端到端）

- `镜像 Sink`（自写测试 util，模式参考 `KafkaJsonRenameStateOperator`）：
  - `CreateTableEvent` → 建镜像表（schema 用连接器的类型映射，保证确定性）；
  - `AddColumn / DropColumn / AlterColumnType` → ALTER 镜像表；
  - `RenameTableEvent` → 迁移镜像表（参考算子的 `perTableState.remove(old).put(new)` 模式）；
  - `DataChangeEvent` → 按主键 UPSERT / DELETE（幂等，天然吸收"已发表去重"事件）。
- `对账器`：源表 vs 镜像表，逐 key 行指纹 + count + 数值列 checksum。
- 命中 P1/P3，是"用户看到的结果对不对"。

### 6.3 收敛断言（最终门槛）

工作负载结束 → 排空（drain）到镜像表静止 → 断言 `镜像 == 源 && 违规 == 0`。

---

## 7. 韧性验证：恢复可观测

- 开 checkpoint；运行中途触发 fail（手动 restart 策略 / 杀 TM 模拟崩溃），恢复后**继续写入**
  再收敛。
- 可观测信号：恢复前后 `lag`、`重复事件计数`（镜像 sink 幂等 UPSERT + Ledger 每 key 计数）、
  收敛是否重新成立。
- 直接命中 P5，是对 EXACTLY_ONCE.md"断点续传"论证的实证。

---

## 8. 场景矩阵：确定性 + 随机化两层

### 8.1 确定性场景（定位具体 bug，可复现）

| # | 场景 | 瞄准 |
|---|---|---|
| S1 | initial + **快照期写入** | ROADMAP P0-4 重复根因 |
| S2 | 多分区 + 大表多 chunk + **边界时刻写** | BOUNDARY_AUDIT F1/F4 |
| S3 | 流中 DDL storm（ADD / DROP / MODIFY / RENAME COLUMN / **RENAME TABLE**） | Plan A + ROADMAP P3-2 |
| S4 | no-PK 表 | 已知限制行为 |
| S5 | checkpoint → kill → 恢复 | P5 |
| S6 | 多库多表 | 路由 / 多源 |
| S7 | 全类型值往返 | ROADMAP P1-1 / P1-2 |
| S8 | 并行度 2+（CreateTableEvent 已知幂等冗余） | ROADMAP P0-2 |

### 8.2 随机化场景（广撒网，防抖）

| # | 场景 | 说明 |
|---|---|---|
| R1 | 随机 DML | 多轮不同 seed |
| R2 | 随机 DML + DDL | 随机 DDL 混合 |
| R3 | 长跑随机 + 中途恢复 | 综合压力 |

### 8.3 覆盖矩阵

`MySQL+canal / TiDB+TiCDC` × `initial / snapshot / stream-only` × `并行度 1 / 2` ×
`单表 / 大表多 chunk / 多表` × `三种写相位`。TiDB 侧复用现有 `TiDBCluster` + `TiCDCServer` 基建。

---

## 9. 验收标准与质量门槛

- 每场景每 seed 跑完：**零违规 + 镜像表 == 源表（收敛）+ Ledger 重放一致**。
- 产出：每场景指标汇总（事件速率、lag、去重命中、违规计数）+ 对账报告落盘。
- 加 `@Tag` 分层（冒烟 / 完整 / 长跑），CI 跑冒烟 + 完整，Docker 不可用时跳过
  （沿用 `KafkaJsonSourceTestBase.checkDockerAvailable` 模式）。

---

## 10. 需新建的测试基建

全在 pipeline 模块 `src/test/` 下，**不动 released**：

| util | 职责 | 参照现有 |
|---|---|---|
| `WorkloadDriver` | DML/DDL 生成 + 期望表模型 + Ledger | `KafkaJsonSourceTestBase` |
| `Ledger` / `LedgerVerifier` | 账本读写 + Mode A 对账 | — |
| `AuditEvent` + emitter 旁路 | es/ts/offset 元数据包装 | `KafkaJsonPipelineRecordEmitter`（改） |
| `MirrorSink` + `Reconciler` | 镜像写 + 源 vs 镜像 diff | `KafkaJsonRenameStateOperator` |
| `MetricsCollector` | MiniCluster 内读指标 | — |
| 场景 ITCase ×N | 继承现有 base，`@Tag` 分层 | `MySqlCanalChainITCase` |

---

## 11. 落地顺序（分阶段）

| 阶段 | 内容 | 命中 |
|---|---|---|
| **V1** | 审计元数据挂载 + 镜像 Sink + Reconciler + S1/S7（MySQL+canal 确定性对账） | 先打通"能观测、能对账" |
| **V2** | WorkloadDriver 随机化 + Ledger / Mode A + 边界场景 S2 | S1/S2 是 exactly-once 核心 |
| **V3** | DDL / RENAME 场景 S3 + no-PK S4 + 多表 S6 | Plan A 特性实证 |
| **V4** | 韧性 S5 / R3 + TiDB parity + 可选 Prometheus/Grafana 长跑观测台 | P5 + 手动可观测层 |

---

## 12. 风险与限制

1. **镜像 Sink 必须自写**（released 没有通用 JDBC sink），且要消费 `CreateTableEvent` /
   `RenameTableEvent` 做 schema 迁移——这正是"裸 source 部署"架构的既定责任，不算越界；
   DDL 支持按 S3 逐步加。
2. **no-PK 表**（文档已知限制）：对账用整行 key，先标记为已知差异。
3. **es 元数据是小扩展**（改 pipeline 模块自己的 emitter），Event 模型与 released 序列化零改动。
4. **随机测试防抖**：固定 seed + 收敛型断言（非精确序列）+ 每场景重试预算；真正的"无重无丢"
   以镜像收敛为准，允许 wire 形状差异。
5. **Docker 重量**：分层 tag，冒烟场景 < 5 分钟，长跑单独 profile。
6. **与现有精确序列测试的关系**：保留为回归；新方案是属性级黑盒校验，两者互补。
