# Debezium 消息接入：规划文档评估与修订方案

> 状态：2026-08-26 评估。来源：aaa.docx 附带的《Debezium 消息接入规划文档》（作者/时间不详）。
> 本文先总结原文要点，再逐项对照当前代码库评估（哪些已实现、哪些冗余、哪些是新价值、哪些有风险），最后给出修订后的分步实施方案。
> 相关文档：[[ROADMAP.md]]（P4 = Debezium，原为低优先）、[[ARCHITECTURE.md]]（现有数据流与事件模型）。

---

## 一、原文总结（规划文档讲了什么）

**背景与目标**：现有连接器已支持 Canal flatMessage 格式，需扩展支持 Debezium 标准格式与 TiCDC（TiDB）Debezium 兼容格式，实现与 Canal 链路「高度解耦」的可插拔解析架构，并支持 TiDB WaterMark 事件。

**六阶段方案**：

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| 一（第1周） | 基础设施：`KafkaJsonMessage` 抽象基类、`KafkaJsonMessageParser` 接口、`KafkaJsonParserFactory` 工厂 | 3 个新类 |
| 二（第2周） | 实体完善与存量适配：完善 `DebeziumMessage`（Schema/Payload/Source/TableChange/TransactionInfo 内部类）、`KafkaJsonFlatMessage` 改名 `CanalMessage`、所有调用点改为走工厂/父类、`KafkaJsonSourceInfo*` 适配两种格式、测试类适配 | 20 项任务（T2.1–T2.20） |
| 三（第3周） | Debezium 解析器：标准 schema+payload 格式、schema-include=false（无 schema 层）、TiCDC 特殊字段（commit_ts/cluster_id） | `DebeziumMessageParser` + 单测 |
| 四（第4周） | 路由与事件处理集成：Converter 支持解析器选择、新增 `debezium-json.format=standard|ticdc` 配置、SchemaChangeHandler 支持 Debezium DDL、EventDeserializer 支持 Debezium 事件、集成测试 | 5 项任务 |
| 五（第5周） | 细粒度 ALTER：`ColumnChangeInfo`、扩展 `KafkaJsonTableChangeType`（ADD/DROP/ALTER_TYPE/ALTER_COMMENT/ALTER_POSITION）、`DdlParsedResult.columnChanges`、Druid `parseAlter` 检测列变更、`compareColumnChanges` | 5 项任务 |
| 六（第6周） | 测试与验证：3 个单测类（各 5–15 例）、3 个集成测试、1 个性能基准；覆盖率 >80% | 测试代码 |

另含：接口规范（4.1–4.3）、数据一致性保障（5）、异常处理（6）、性能优化（7）、安全防护（8）、测试计划（9）、交付清单（10）、风险评估（11）。

---

## 二、与现状对照评估

### 2.1 已经实现（规划文档视为新工作，实际已落地）

对照当前代码（分支 `feature/canal-rename-plan-a`，含 aaa.docx 代码变更后的 `b46a8e15`），以下内容**已经存在**，规划文档把它们当成待办是**过时**的：

| 规划文档条目 | 现状 | 证据 |
|---|---|---|
| 阶段五全部（ColumnChangeInfo / 枚举扩展 / columnChanges / Druid parseAlter / compareColumnChanges） | ✅ 已实现并带单测（正是上批 aaa.docx 代码变更） | `source/ddl/ColumnChangeInfo.java`、`KafkaJsonTableChangeType`（含 ADD_COLUMN/DROP_COLUMN/ALTER_COLUMN_TYPE/ALTER_COLUMN_COMMENT/ALTER_COLUMN_POSITION）、`KafkaJsonDdlParsedResult.columnChanges`、`KafkaJsonDruidDdlParser.parseAlter` |
| `MessageFormat` 枚举（CANAL/DEBEZIUM） | ✅ 已存在 | `KafkaJsonSourceOptions.MessageFormat` |
| 消息格式配置项（原文提议 `debezium-json.format`） | ✅ 已存在，但命名不同：`scan.message.format=canal|debezium` | `KafkaJsonSourceOptions.MESSAGE_FORMAT` |
| TiDB WaterMark 剔除（原文 T2.6 的 `op=="m"` 是 Debezium 格式口径） | ✅ Canal 格式已剔除 `TIDB_WATERMARK`（`type=TIDB_WATERMARK`） | `KafkaJsonRecordConverter.convert` case "TIDB_WATERMARK"（P2-1 已落地） |
| TiDB TSO → 时间戳 / 快照水印 | ✅ 已实现 | `KafkaJsonTidbOffsetUtils`（P2-2 已落地） |
| `scan.database.type` 新增 TIDB | ✅ 已实现（复用 MySQL 方言） | `KafkaJsonSourceOptions.DatabaseType.TIDB` |
| Debezium DDL 解析器 | ✅ 已实现（ANTLR） | `KafkaJsonDebeziumDdlParser` + `scan.ddl.parser=debezium` |
| TiCDC / canal 端到端集成测试 | ✅ 已实现（Phase 11） | `MySqlCanalChainITCase`、`TiDBCdcChainITCase`、`TiDBSnapshotSimulatedITCase` 等 |
| 阶段六「性能基准 / 覆盖率 >80%」 | ⚠️ 未做，但不可在本环境跑（见 2.3） | — |

### 2.2 规划文档真正的新增点（值得做）

| # | 新增点 | 说明 |
|---|--------|------|
| N1 | **Debezium 格式 DML 的消费**（核心） | 现在 `scan.message.format=debezium` 是 fail-fast 拒绝的（`KafkaJsonSourceConfigFactory` 第 96 行）。Debezium 消息自带 schema+payload 信封、typed before/after、source.ts_ms，与 canal 的 string 行完全不同，需要一个 `DebeziumMessageParser` + 转换路径把它变成现有的 Debezium-shaped `SourceRecord`。**这是整个规划文档最有价值、也是最大的缺口。** |
| N2 | **EventTime.TIDB_TSO** | 现有 `EventTime{ES,TS}` 没有 TIDB_TSO；Debezium 消息有 `payload.ts_ms` 与 `payload.source.ts_ms` 两个时间（与 canal 的 es/ts 对应），需评估映射并统一 `KafkaJsonKafkaOffsetUtils.extractEventTime` / `KafkaJsonRecordConverter.eventTime`。 |
| N3 | **Debezium WaterMark（op=="m"）** | TiCDC 的 Debezium 兼容格式（enable-tidb-extension=true）以 `op:"m"` 发 watermark，需识别为 `TIDB_WATERMARK` 而非 DML。 |
| N4 | **解析器工厂可插拔** | `KafkaJsonMessageParser` 接口 + `KafkaJsonParserFactory`：当前 `KafkaJsonFlatMessageParser.parse()` 是静态直调，工厂化后 `scan.message.format` 一处决定解析器。 |
| N5 | **Debezium DDL 消息接入** | Debezium 的 DDL 以 schema-change 事件（或 history topic）下发，SQL 文本可复用现有 Druid/Debezium DDL parser 链路。注意 TiCDC 8.x **不发** Debezium DDL（ROADMAP P4-2），仅标准 Debezium 有此路径。 |

### 2.3 规划文档的问题与风险（评估意见）

1. **「消息抽象层」过度设计，且与现有代码冲突。**
   - 原文 `KafkaJsonMessage` 里 `getEventTime()` 出现两次（一次返回 `EventTime`、一次返回 `Long`），类图里又变 `getEventTimeValue()`——签名是坏的；`EventTime`/`MessageType` 又打算复用 `KafkaJsonSourceOptions.EventTime` 又自己内嵌定义，语义重复。
   - 更根本的：canal 与 Debezium 的**值表示**不同（canal 是 string 行，Debezium 是 typed struct）。原文自己也承认 Converter/TableUtils 要「通过 instanceof 判断」——一旦引入 instanceof 分派，抽象基类就没有多态价值，只是多一层间接。
   - **修订建议**：保留「接口 + 工厂」的**选择层**（这是可插拔的关键），但**不做** `KafkaJsonFlatMessage→CanalMessage` 改名（纯 churn、破坏模块命名惯例）、不做 `getSchemaName()`（MySQL 恒 null，无消费方）、不做统一父类上的 instanceof 大杂烩。解析器产出各自的消息类型，流式任务只依赖少量公共 getter。

2. **阶段四配置项与现状冲突。** 原文提议 `debezium-json.format=standard|ticdc`；现状已有 `scan.message.format=canal|debezium`。新增独立维度无必要——`standard` vs `ticdc` 的差异（commit_ts/cluster_id/op==m）**可由 parser 自动探测**（消息里有 `payload.source.commit_ts` 或 `cluster_id` 即 TiCDC 形状），不需要第二个开关。修订方案不引入该配置。

3. **性能目标不可在本环境验收。** 「10万 msg/s、P99<5ms、<512MB」是压测目标，本仓库（WSL2、无压测环境）无法运行 `BenchmarkTest`；且 `treeToValue` 全量绑定 schema 的场景与原文自己 7.2 的「Schema 字段缓存」矛盾。修订方案**砍掉性能基准**，以正确性测试为准。

4. **schema-include=false 的边界。** 无 schema 层时列类型只能靠 `payload` 的 value 反推或走 JDBC 已知表 schema（本连接器已有 JDBC 快照 schema 缓存 `KafkaJsonRecordFactory`）。正文 T3.2 只给了「包一层 payload」的绑定技巧，没回答类型来源——修订方案明确：**优先用已注册表 schema（JDBC/DDL），payload 只供取值**。

5. **偏移/水印域问题（原文 T2.19 自认）。** Debezium 的 `ts_ms` 与 canal 的 `es` 同域（Unix 毫秒），可映射；但 `offsetsForTimes` seek 用的是 **Kafka 消息时间戳**，与消息内容时间是两个域（P2-2 已记录此遗留）。把 Debezium `ts_ms` 接进 `extractEventTime` 后，快照边界语义与 canal 一致，属安全；`TIDB_TSO` 模式则只对 `es` 域成立（沿用现有约束）。

6. **风险：Debezium 消息的 typed 值 → 现有 `valueFromColumnData` 的转换。** canal 走 string→`KafkaJsonValueConverter`；Debezium 的 before/after 已是 Kafka-Connect 风格 JSON（date=天偏移、timestamp=微秒、decimal=string/bytes、TiDB decimal=float64——ROADMAP P4-1 记录的坑）。这块转换层是 S3 的技术难点，需要单独的转换器，**不做** string 化往返（丢精度）。这也是为什么 canal/Debezium 不能共用一个"行"模型。

7. **TiCDC 8.x 无 Debezium DDL 事件**（ROADMAP P4-2 已核实）。所以「Debezium DDL」只对标准 Debezium（MySQL+Debezium Connect）成立；TiDB 侧 DDL 仍走 canal-json（已支持）。修订方案在 S3 里对 Debezium DDL 事件做基础接入（SQL 文本走现有 parser），但不承诺 TiDB 侧 Debezium DDL。

---

## 三、修订后的实施方案

> 原则：复用现有结构，只补真正的缺口；`scan.message.format` 是唯一格式开关，不做第二个配置维度；每步编译 + 单测全绿再进下一步。

> 进度：S1 ✅（193 绿）、S2 ✅（206 绿）、S3 ✅（225 绿）、S4 ✅（pipeline 模块单测 24 绿 + `KafkaJsonDebeziumSimulatedChainITCase` 端到端通过）。

### S1 消息抽象层（最小集）+ 解析器工厂
- `EventTime` 加 `TIDB_TSO`。
- `KafkaJsonMessage` 抽象类：`MessageType`（DDL/DML/TIDB_WATERMARK/UNKNOWN）、`getDatabase()`、`getTable()`、`getSql()`、`getEventTimeValue(EventTime)`（复用 `KafkaJsonSourceOptions.EventTime`，不重复定义）。
- `KafkaJsonMessageParser` 接口（`parse(String)` / `getFormat()`）+ `KafkaJsonParserFactory.create(MessageFormat)`。
- `KafkaJsonFlatMessage extends KafkaJsonMessage` 实现 getter；空串 db/table → null（注意：flat message 的 db/table 是原始字段，空串归一化只发生在 Debezium 侧，见 S2）。
- `CanalMessageParser implements KafkaJsonMessageParser<KafkaJsonFlatMessage>` 委托现有解析逻辑（`KafkaJsonFlatMessageParser` 保留为静态工具）。
- 单测：工厂选择、flat message getter、空串归一。

### S2 DebeziumMessage 实体 + DebeziumMessageParser
- `DebeziumMessage`（Jackson，`@JsonIgnoreProperties(ignoreUnknown=true)`）：`schema`/`payload`；`payload.source.{db,table,ts_ms,commit_ts,cluster_id}`、`payload.{op,before,after,ts_ms,ddl}`。
- `DebeziumMessageParser`：标准格式（schema+payload）、schema-include=false（无 schema 层：原样绑 payload）、TiCDC 自动探测（`source.commit_ts`/`cluster_id` 存在即 TiCDC）；`op=="m"` → `TIDB_WATERMARK`；空 db/table → null。
- `getEventTimeValue(ES/TS)` 带跨字段回退（ES→source.ts_ms 缺则取 payload.ts_ms；TS 反之），保证无 source.ts_ms 的消息（如裸 schema-change record）仍有可用的排序键。
- 单测：标准 DML、无 schema、TiCDC DML、watermark、空 db/table、非法 JSON。

### S3 流式链路接入
- `KafkaJsonStreamFetchTask`：从 `sourceConfig.getMessageFormat()` 建 parser，`processRecords` 用它解析；事件时间按格式取（canal es/ts；debezium ts_ms）。
- `KafkaJsonRecordConverter`：加 `convert(KafkaJsonMessage, …)` 分派——canal 走现有路径；debezium 走 `convertDebezium`（typed before/after → `KafkaJsonRecordFactory.debeziumRowData` → `createRecord`，**只**用已注册表 schema，类型转换见 P4-1 笔记）。
- `KafkaJsonValueConverter.convertFromJson(Column, JsonNode)`：De bezium 类型化值转换——数字→文本回灌 canal 转换器；epoch 编码的时间类型（DATE=天、TIME=毫秒、DATETIME=微秒、TIMESTAMP=毫秒，按 adaptive 假设）；布尔→Boolean；JSON 列嵌套→compact JSON；二进制 base64 文本→byte[]。**DECIMAL 不支持 `decimal.handling.mode=precise`（base64 字节），用 double/string 或 TiCDC。**
- `KafkaJsonKafkaOffsetUtils.extractEventTime`：按格式取 es/ts 或 payload.ts_ms；`KafkaJsonOffsetSupplier` 传 `messageFormat`。
- `KafkaJsonSourceConfigFactory`：放行 `scan.message.format=debezium`（fail-fast 只剩 databaseType 检查）。
- 单测：debezium 消息 → SourceRecord 形状断言（before/after/op/source/ts_ms）、epoch/文本时间值、offset 采样、TS 模式。
- **实施笔记 / 已知取舍**
  - Debezium DML 依赖已注册表 schema（快照阶段先注册），**流式-only 无快照场景不适用**——debezium 消息无 mysqlType，无法 buildTable 兜底，未注册时丢弃并告警。
  - 无任何时间戳的 Debezium DDL（裸 `{databaseName,ddl}`）事件时间为 -1，会被低水位过滤丢弃（与 canal DDL 的 es<low 语义一致：该 DDL 的 schema 已含在快照里）。有 `ts_ms` 的 DDL 正常处理。
  - TiDB+Debezium 建议配 `scan.event-time=tidb-tso`：边界（TSO 物理毫秒）与消息事件时间（commit_ts>>18，同为 TSO 物理毫秒）同尺度。配 `es` 时边界走 TSO、消息走 Unix 毫秒，尺度不一致（与 canal 格式既有行为一致，已知取舍）。

### S4 Pipeline 层打通 + 端到端
- 验证 pipeline `KafkaJsonEventDeserializer`（继承 `DebeziumEventDeserializationSchema`）原生消费 Debezium-shaped SourceRecord，无需改代码；必要时补序列化/类型测试。
- 模拟 Debezium 消息端到端测试（source → pipeline Event），与 Phase-11 的模拟链路同构。
- **实施笔记**：`convertDebezium` 复用与 canal 相同的 `KafkaJsonRecordFactory.createRecord`，产出的 SourceRecord 值 schema 与 canal 路径字节一致，因此 `DebeziumEventDeserializationSchema`（读 `op`/`source.db`/`source.table`/`before`/`after`）无需任何适配即可消费。`KafkaJsonDebeziumSimulatedChainITCase` 实证：真实 MySQL 快照（注册表 schema）→ 模拟 Debezium 信封（INSERT/UPDATE/DELETE，typed before/after + `source.ts_ms`）→ 增量 Event 序列与 canal 基线一致。

---

## 四、与 ROADMAP 的关系

- ROADMAP P4（Debezium）原排「低优先 / 等 ticdc 9.0」。本方案是 P4 的执行细化，优先级由用户本次指示上提。
- P4-1（TiDB debezium 类型转换：decimal=float64、datetime UTC 语义）是 S3 的**前置依赖**，已在 S3 中纳入。
- P4-2（TiCDC debezium schema change）维持原结论：8.x 不支持，本方案只做标准 Debezium 的 DDL 接入。
- 本方案不引入 `debezium-json.format` 独立配置（见 2.3-2）。
