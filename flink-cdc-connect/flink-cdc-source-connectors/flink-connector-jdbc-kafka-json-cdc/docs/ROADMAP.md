# jdbc-kafka-json-cdc Roadmap / 优先级规划

> 状态：2026-08-13 排定。Phase 11 集成测试已全绿并提交（f48f1e8d）。
> 总原则（用户拍板）：**先保证数据 exactly-once，再保证数据转换正确性；Debezium 相关不急。**

优先级：P0 = exactly-once 正确性（阻塞性）｜P1 = 数据转换正确性｜P2 = TiDB 链路补强｜P3 = 健壮性 / DDL 扩展｜P4 = Debezium（低优先）

---

## P0 — Exactly-Once 正确性（先做，阻塞后续所有链路可信度）

### P0-1 流式边界语义审计与修复（`KafkaJsonStreamFetchTask#processRecords` / END watermark 归属）

**现象/疑问：**
- 当前 `processRecords` 消费**一整批**后才判断 `currentOffset >= endingOffset`，为真则 `dispatchEndWatermark`，且传入的也是 `endingOffset`。处于 `>= endingOffset` 的**端点消息也会被 emit**（EXACTLY_ONCE.md 记录的语义是回填覆盖 `(LOW, HIGH]` 含端点）。
- 潜在重复：端点消息 emit 之后，恢复 / 下游重放是否会再读到一次？
- 用户提出的两个候选修法：
  1. 循环中一旦看到数据 `> endOffset` 就**剔除**该条并直接终止循环返回 `true`（不 emit 越界数据）；
  2. 越界数据的判定不应依赖"整批消费完再回头看"，应在单条粒度处理。

**架构问题：END watermark 到底该谁派发？**
- 当前是 `KafkaJsonStreamFetchTask` 自己调 `dispatchEndWatermark`。
- flink-cdc-base 的原始设计里，快照 split 的 END watermark 由 `AbstractScanFetchTask`（scan fetch task）在快照 split 读完后派发；本连接器因为**回填（backfill）复用了 `KafkaJsonStreamFetchTask`**，所以流式任务直接派发 END 在语义上是否与 base 的 split 状态机冲突，需要核对：
  - `AbstractScanFetchTask` 在快照读完后如何决定进入 backfill vs 派发 END；
  - 谁负责把 StreamSplit 标记 finished、谁触发 `StreamSplitState` 收敛。

**验收标准：** 写一个专门的多轮 checkpoint + 恢复测试（source 从快照切增量、中间 failover），断言端到端无重复、无丢失；或至少在 `KafkaJsonStreamFetchTaskTest` 级别用可控的 consumer 模拟越界消息，断言边界行为。

**结论与修复（2026-08-13，已落地未提交）：**
- **END watermark 归属：不是 bug。** base 的 `AbstractScanFetchTask.execute` 只在 `!streamBackfillRequired`（low==high）时自己派发 END；一旦进入 backfill 就调用 `executeBackfillTask`，由其负责派发 END。`KafkaJsonStreamFetchTask` 在有界回填里 `dispatchEndWatermark` 与 MySQL 连接器模式一致，属正确。
- **真正的 bug：有界回填整批超发。** 旧 `processRecords` 消费**一整批**后才看 `currentOffset >= endingOffset`，因此 es ≥ HIGH 的消息（快照期间新到的变更）会被回填发出；而流式 split 从「事件时间含端点的 starting offset」`offsetsForTimes(HIGH)` 开始读，`IncrementalSourceStreamFetcher.shouldEmit` 的 `isAtOrAfter(table 的 maxSplitHighWatermark)` 对这批 es≥HIGH 记录再放行一次 → **重复**。MySQL 连接器不会（binlog 读到首个 ≥HIGH 事件即停），本连接器按整批处理就会越界。
- **修复（`KafkaJsonStreamFetchTask`）：** 有界读把 ending offset 当**排他**上界——es ≥ ending 的消息一律不 emit（归流式阶段，流式从含端点的起点读，正好补上，无丢失）；终止条件是**每个**已分配 partition 都出现 es ≥ ending（或排空到 log end）才 `dispatchEndWatermark`，不能见首个越界就停（否则同批其它 partition 未读的 es<ending 消息会丢）。
  - 单 split：回填发 `[LOW, HIGH)`，流式发 `[HIGH, …)`，恰一次。
  - 多 split：es≥HIGH_i 的消息由该表自己的 pure-stream-phase（`isAtOrAfter` 含端点）补发，无重复无丢失。
  - 空 partition（end==0）直接视为 done，避免挂死。
- **测试：** `KafkaJsonStreamFetchTaskTest` 新增「两个 partition，一个先越界、另一个还有 es<ending 未读」用例（`FakeKafkaConsumer` 加了 `maxRecordsPerPoll` 支持增量 poll）；`testBoundedReadDispatchesEndWatermark` 改为断言 es==ending 的记录**不** emit；`KafkaJsonScanFetchTaskTest` 全管道用例改为 6 条（端点记录不再由回填发）。**4 个 Phase-11 ITCase 全绿。**
- **已知边角（与 base 一致，未修）：** snapshot-only 模式下 exact-端点记录的行为；es 非单调（canal 乱序）时的理论边界。均有注释。

### P0-2 split 完成后仍读 Kafka 数据 + 重复 CreateTableEvent（多 subtask）

**结论与修复（2026-08-13，已核实）：**
- **"Finished reading split 仍然会读取 kafka 数据"：不是 bug。** 快照 split 全部读完、END watermark 派发后进入**流式阶段**，由 stream split 的 subtask 从起点持续消费 Kafka 增量——这正是设计的正常行为。`IncrementalSourceStreamFetcher.configureFilter` 用的是 **stream split 里携带的全局 finished-snapshot-split 信息**（由唯一的 `HybridSplitAssigner` 在 coordinator 侧汇总，跨 subtask 全局），`shouldEmit` 的 pure-stream-phase 判定按**每表全局 max high watermark** 过滤，与哪个 subtask 做快照无关。**但"并行度 > 1 时数据 exactly-once"的早期结论被用户实测推翻**：min 水位 + 排他上界会把读库期间提交的变更漏到流式侧重复下发（见 P0-4，已修复）。
- **重复 CreateTableEvent（来自不同 subtask）：真实存在但幂等，且与官方 MySQL 连接器行为一致。** 根因确认为候选 1：`KafkaJsonPipelineRecordEmitter` 的 `alreadySendCreateTableTables` 是**按 emitter 实例（subtask）隔离**的普通 HashSet。快照阶段每个表由其 snapshot split 的 LOW watermark 发射一次 CreateTableEvent；流式阶段由 stream split 的 subtask 在 split 开始处**再发射一遍缓存里该 subtask 没发过的全部表**（含快照落在其它 subtask 上的表）→ 重复。该"再发射"是设计内的安全网（stateless 下游 / 恢复后需要重宣告 schema），与官方 `MySqlPipelineRecordEmitter` 完全同构，不能直接删。在**标准 pipeline 框架**（含 SchemaOperator）下无害：下游 `SchemaManager.isOriginalSchemaChangeEventRedundant` 对重复 CreateTableEvent 判为 redundant 忽略，幂等。**在裸 source 用法下**（用户把 pipeline 模块当 Flink source 直接 `env.fromSource`，下游无 SchemaManager），重复事件会直达用户 sink。
- **处理（已落地）：** 裸 source 用法建议**并行度 = 1**（此时每表恰一次 CreateTableEvent、schema/DDL 与数据严格有序）；并行度 > 1 仅当下游对 CreateTableEvent 幂等（CREATE-IF-ABSENT / OR-REPLACE）时使用。跨 subtask 去重需要 distributed state，emitter 拿不到（也不碰 released 模块），与官方一致不予实现。**DML 的 exactly-once 由 P0-4 的水位修复保证**（并行度 > 1 下同样成立，因 `shouldEmit` 的端点判定用全局 max high watermark + sentinel 过滤）。

**验收标准：** 并行度 2+ end-to-end 无重复 DML（P0-4 已保证）；CreateTableEvent 重复为已知幂等冗余，标准 pipeline 下由 SchemaManager 吸收。

### P0-3 表 schema 注册并行化问题

**结论与修复（2026-08-13，已落地未提交）：**
- **现象部分成立：同一张表的 schema 解析/注册存在真实的线程安全缺陷。** 共享对象是 `KafkaJsonDialect` 持有的 `KafkaJsonSchema`（含 `schemasByTableId` HashMap + `KafkaJsonRecordFactory`）——`IncrementalSource` 把 `dataSourceDialect` 作为**单实例**传给所有 subtask，因此该 schema 被两类线程并发访问：
  1. **enumerator 线程**：`JdbcSourceChunkSplitter`（快照分 chunk）在 `SnapshotSplitAssigner` 里逐表调 `dialect.queryTableSchema`；`scanNewlyAddedTable=true` 时还会周期 `checkSplitChanging` 再触发；
  2. **stream-split reader 线程**：stream split 的 `tableSchemas` 是空 map（`HybridSplitAssigner.createStreamSplit` 传 `new HashMap<>()`），`IncrementalSourceReader.discoverTableSchemasForStreamSplit` 必然调 `dialect.discoverDataCollectionSchemas` → `queryTableSchema`。
  两者并发写同一 HashMap / 注册同一 `KafkaJsonRecordFactory` → 数据竞争（HashMap 可能损坏）。默认配置下时序上多为串行（enumerator 先 chunk 完再发 stream split），但 scanNewlyAddedTable 或任何并发触发即踩坑；懒初始化的 `schema`/`filters` 字段本身也不是线程安全的。
- **修复（source 模块内，不改 released 模块）：**
  - `KafkaJsonDialect`：`schema`/`filters` 改 `volatile` + synchronized 双重检查懒初始化；
  - `KafkaJsonSchema`：`getTableSchema`/`registerTable`/`removeTable` 加 `synchronized`（共享实例的**唯一** mutation 入口即 `getTableSchema`，锁住它即可串行化所有并发 schema 解析；per-subtask schema 单线程不受影响，加锁开销可忽略）。
- **per-subtask 的 schema/recordFactory 本身无跨 subtask 共享**（`KafkaJsonSourceFetchTaskContext.configure` 每 subtask 各建一份），故读取路径（快照读、流式转换、DDL handler）天然隔离；`KafkaJsonRecordConverter`/`KafkaJsonSchemaChangeHandler` 全部操作的是各自 subtask 的实例。竞态仅限 dialect 共享实例的解析入口。
- **验证：** source 模块 144 单测全绿（含并发改动后回归）。

**验收标准：** 多并行度下同一表的 schema 解析不会损坏缓存（并发写已串行化）；`scanNewlyAddedTable=true` 与 stream 发现并发时不再有 HashMap 竞争。

### P0-4 快照↔流式衔接重复：min 水位把「读库期间提交的变更」漏到流式侧（用户实测复现）

**现象（用户 2026-08-13 实测）：** 快照阶段写入一条 UPDATE，之后程序（JDBC 快照读）查询到它——快照行已含该变更的效果；这条 canal 日志在 Kafka 里被程序**完成快照阶段后再次捕获发到下游**，出现重复。

**根因（已确认，推翻 P0-2 的早期"已 exactly-once"结论）：** `KafkaJsonKafkaOffsetUtils.queryCurrentOffset` 返回各 partition 最新消息 es 的**最小值** `(min, -1, -1)`，用作快照 split 的 HIGH 水位。快照读库期间提交的变更 E 若 `min < E ≤ 真实边界`：
- 有界回填以 `es ≥ HIGH`（= min）为排他上界 → 把 E 剔除，不覆盖；
- 流式 `shouldEmit` 的 `isAfter((min,-1,-1))` → 把 E 再放行；
- 而 JDBC 快照已读到 E 的效果 → 下游收到「快照行 + 同一变更」= **重复**。
min 水位只保证"不丢"，靠 HIGH 去重；但排他上界 + min 水位恰好把读库期间的变更推出回填窗口又不被去重。MySQL 连接器无此问题（binlog 位置单调且真实）。

**修复（source 模块内，2026-08-13 已落地未提交）：**
1. `queryCurrentOffset` 改为各 partition 最新消息 es 的**最大值** + sentinel 分区/偏移 `(max, Integer.MAX_VALUE, Long.MAX_VALUE)`。真实记录分区恒 < MAX，故 es==max 的边界记录排在水位**之前**（`isAfter` 为 false）→ 归属回填。同时更新 `KafkaJsonOffsetSupplier` 与 `KafkaJsonKafkaOffsetUtils` 的 javadoc。
2. `KafkaJsonStreamFetchTask` 有界读：
   - 上界从 `es ≥ ending` 改为 `lastOffset.isAfter(endingOffset)` —— 边界记录含端点、由回填恰好发出一次；
   - 新增下界 `es < starting` 一律丢弃 —— 快照前的变更（canal 延迟后到、seek 又读到了它）若覆盖快照行，可能把其已被后续变更 supersede 的值回退回去。
   - 语义变为：单 split 回填发 `[LOW, HIGH]`（含端点），流式发 `(HIGH, …)`；读库期间提交的任意变更都落在 `(low, high]` 窗口内，由回填覆盖/新增/删除恰一次，流式不再重放（基座 `rewriteOutputBuffer` 对 CREATE/UPDATE 是 `put`、DELETE 是 `remove`，故新行不丢、改行覆盖、删行移除）。

**验证（2026-08-13）：** `KafkaJsonStreamFetchTaskTest` 新增 2 例——(a) es==ending 的边界消息被回填保留（用户场景的直接回归）；(b) es<starting 的 canal 滞后消息被下界丢弃。`KafkaJsonOffsetSupplierTest` min→max 断言更新。source 模块 155 单测全绿。4 个 Phase-11 ITCase 不受影响（快照期 topic 为空 → 水位仍是 `INITIAL_OFFSET`）。

**残余（与 base 一致，未修，已注释）：** 多 split 之间某行的变更若其 chunk 已读完，依赖下一 split 的 JDBC 读补**终值**（变更形状可能丢失但终值正确）；canal 延迟超过有界回填窗口同理。均不以重复换取，不丢终值。

---

## P1 — 数据转换正确性

### P1-1 `KafkaJsonValueConverter` 常见类型数值转换测试

**目标：** 补齐常见类型转换正确性测试：`int`、`bigint`、`timestamp`、`datetime`、`date`、`decimal`、`double`、`varchar`、`time`、`timestamp with zone`（含 0 日期、NULL、边界值、精度）。
**做法：** 扩展 `KafkaJsonValueConverterTest`（如存在）或新增，逐类型断言 `convert(column, value)` 的返回值类型与取值；与 MySQL 8 / TiDB 真实值对照。
**注意：** 这正是 P2-2 / P4 各种坑（UTC、float64、fraction 位数）的收敛地——转换器一旦被测试锁定，后续 TiDB 特例直接在此扩展。

**状态（2026-08-13，已落地未提交）：** `KafkaJsonValueConverterTest` 由 12 例扩到 21 例，全部通过（source 模块 153 例全绿）。新增覆盖：
- `testIntegerBoundariesPassThrough`：INT/BIGINT 正负极值（String 直通）；
- `testFloatAndDoublePassThrough`：FLOAT/DOUBLE、科学计数法 `1.79E308`、`DOUBLE UNSIGNED`（不落入整数 Number 路径）；
- `testDecimalPrecisionAndScalePassThrough`：DECIMAL(38) 大精度、`DECIMAL UNSIGNED` 小数直通（核验 `KafkaJsonTableUtils.isUnsigned` 只认 5 种无符号整数，小数/浮点不会被 `Long.parseLong` 误伤——**无 bug，测试锁定行为**）；
- `testVarcharEdgeCases`：空串、Unicode、保留空格；
- `testTimeWithFraction`：TIME(6) 小数秒 → Duration（含纳秒）、负时间小数逐分量取负；
- `testDateBoundaries`：`1000-01-01` / `9999-12-31`；
- `testDatetimePrecisionAndBoundaries`：DATETIME 上下界 + 微秒精度；
- `testTimestampWithZoneLeapDayAndMaxPrecision`：TIMESTAMP 闰日 + 微秒（UTC）；
- `testInvalidValuesThrow`：锁定错误契约——无符号整数非数值、非法日期/时间/时间戳分别抛 `FlinkRuntimeException`（TIME 的 `stringToDuration` 抛裸 `RuntimeException`）。

### P1-2 `KafkaJsonTypeUtils` Datetime / Timestamp 转换异常 + 列长超限

**现象：** Datetime/Timestamp 转换有异常；"使用列长，超过限制"——列长（precision/长度）超过类型上限时转换会出错，需要按不同数据库扩展类型转换工具类。

**方向：**
- 定位 `KafkaJsonTypeUtils`（source 模块 `source.utils`）里 Datetime/Timestamp 按列长派生类型的逻辑，处理超过 MySQL/TiDB 精度上限的场景；
- 按数据库（mysql / tidb）扩展类型转换，把方言差异隔离出来。

**状态（2026-08-13，已落地未提交）：** 根因 = JDBC 元数据对 temporal 报的是**显示宽度**（MySQL 8.0.46 实测 DATETIME(6)→26、DATETIME(3)→23、DATETIME→19、TIME(6)→15、TIME(3)→12；TiDB 同），超过 Flink `TIMESTAMP(p)`/`TIME(p)` 的 p∈[0,9] 上限 → 快照建表 `ValidationException`；且 `DECIMAL_DIGITS`(scale) 对 temporal 恒为 0，拿不到 FSP。修复（见 P1-3 的 `KafkaJsonColumnMeta`）：**从显示宽度反推 FSP**——DATETIME/TIMESTAMP `width>19 ? width-20 : 0`（19→0、23→3、26→6），TIME `width>8 ? width-9 : 0`（8→0、12→3、15→6）。关键：canal 消息路径（`KafkaJsonTableUtils.buildColumn` 解析 DDL 文本）的 length 已是真 FSP（"datetime(3)"→3），与 JDBC 路径的显示宽度语义不同；两者以 `length≤6`（FSP 上限）分界，因为 FSP≤6 而最小显示宽度 8/19。修复后快照(23→3)与流(3→3)发出的 DataType 精度**一致**（修复前快照错成 6）。其余：DECIMAL 无有效精度→(38,18) 否则 scale 钳到 [0,precision]，CHAR/VARCHAR/BINARY/VARBINARY/BIT→`max(1,precision)`。`KafkaJsonColumnMetaTest` 扩到 10 例（双路径 3/6/0 全覆盖），`KafkaJsonTypeUtilsTest` 13 例全绿。

### P1-3 两份 `KafkaJsonTypeUtils` 整合简化

**现状：** pipeline 模块与 source 模块各有一份 `KafkaJsonTypeUtils`——pipeline 版返回 `org.apache.flink.cdc.common.types.DataType`（cdc 类型），source 版返回 `org.apache.flink.table.types.DataType`（flink table 类型）。

**方向：** 评估能否抽一个"列定义 → 中间表示"的公共转换（列元数据 → 独立于两个 DataType 体系的描述），再分别映射到 cdc / flink table 两种 `DataType`，消掉重复的 MySQL/TiDB 方言映射逻辑。
**注意约束：** 只改当前两个模块，不动 released 模块；整合后必须保持现有 SQL 层与 pipeline 层行为一致（全量单测回归）。

**状态（2026-08-13，已落地未提交）：** 新 `source/utils/KafkaJsonColumnMeta.java` 作为唯一共享映射（归一化 + clamping + cdc DataType 映射）；source `KafkaJsonTypeUtils.fromDbzColumn` 纯委托 released `DataTypeUtils.toFlinkDataType(meta.toCdcDataType(col.isOptional()))`；pipeline 副本删除，调用点（`KafkaJsonEventDeserializer` 268/332、`KafkaJsonSchemaUtils.toColumn`）直用 `fromColumn(...).toCdcDataType(...)`。新增 `KafkaJsonColumnMetaTest` 10 例，`KafkaJsonEventDeserializerTest` 11 例全绿；两模块 surefire 零失败。

---

## P2 — TiDB 链路补强

### P2-1 TiDB canal 消息额外处理

**目标（TiCDC canal-json 消费侧适配）：**
1. **剔除 `TIDB_WATERMARK` 类型消息**（`isDdl=false` 且 `type=TIDB_WATERMARK` 时不是 DML）——否则会被当成伪 DML 数据下发。当前 `KafkaJsonFlatMessageParser` 需要识别并过滤。参考：https://docs.pingcap.com/tidb/dev/ticdc-canal-json.md
2. **DELETE 事件兼容**：v5.4.0 起 TiCDC 的 DELETE 事件 `old` 为 `null`、被删数据在 `data` 里（早期版本 `old` 内容与 `data` 相同）。我们的 DELETE 处理读的是 `data`（`canal puts the (before) row into data for DELETE`），需确认对"old=null"路径无副作用。

**验收标准：** `TiDBCdcChainITCase` 或新增单测覆盖一条真实 `TIDB_WATERMARK` 消息 + 一条 TiCDC DELETE（old=null）消息。

**状态（2026-08-13，已落地未提交）：**
- **TIDB_WATERMARK**：`KafkaJsonRecordConverter.convert` 显式 `case "TIDB_WATERMARK"` 过滤并注释（TiCDC 不提供内置过滤，消费方必须剔除）。此前走 `default` 分支恰好也返回空，现显式化锁死语义。新增单测 `testTidbWatermarkProducesNoDataRecords`（真实形状：`isDdl=false`、`type=TIDB_WATERMARK`、`data=null`）。
- **DELETE old=null**：确证无副作用——`convertRows` 的 DELETE 分支始终以 `data`（before 行）作前像，`old` 为 null/absent 无影响（原有 `testDeleteUsesDataAsBefore` 已覆盖，新加 `testTidbDeleteWithNullOldUsesDataAsBefore` 锁定 TiCDC 形状并顺带断言 `source.es`/`source.ts`）。
- **端到端实证：** `TiCDCServer.createChangefeed` 的 sink-uri 加 `enable-tidb-extension=true`（TiCDC **只有**在该扩展开启时才发 TIDB_WATERMARK），`TiDBCdcChainITCase` 全绿——真实链路里 watermark 被正确剔除、DML 的 `_tidb` 扩展字段被 parser 容错、DELETE 正常。

### P2-2 TiDB TSO → 时间戳（event_time / 水印基础）

**背景：** 双水印算法的核心 `event_time` 目前取自 canal 消息的 `es`/`ts`。TiDB 链路上真实 TiCDC canal-json 可能没有可靠的 `es`，需要用 TSO 转时间戳作为 event_time 来源：
- `SELECT TIDB_PARSE_TSO(...)` / `SELECT FROM_UNIXTIME((tso >> 18) / 1000)`（前 18 位为毫秒时间戳）；
- 快照启动时也可查询当前 TSO 来确定 Kafka 消费起点（注意：SQL 查询当前 TSO 需要开启事务）；
- 需要解析消息里的 TSO 字段：canal 格式 `_tidb.commitTs`、debezium 格式 `payload.source.commit_ts`。

**依赖关系：** 与 P2-1（TiDB 消息解析）共用同一批解析代码；先确认真实 TiCDC canal-json 里 `es`/`ts` 到底有没有、准不准——**如果缺失，本项应升为 P1**（影响 TiDB 链路 exactly-once 边界）。

**前置调研结论（2026-08-13，P2-1 完成时）：** 真实 TiCDC canal-json（DML 与 TIDB_WATERMARK 事件都带）里 `es`（事件/执行时间）与 `ts`（发送时间）**都是可靠 Unix 毫秒**，不缺失。**P2-2 不升级 P1**，维持原优先级；TSO→时间戳仍可作为快照消费起点的优化项，但非 exactly-once 必需。

**状态（2026-08-13，已落地未提交）：** 采纳用户提议——TiDB 链路的快照低/高水印改为**直接查数据库当前 TSO**，替代「消费 Kafka 最新消息的 es/ts」：
- 新增 `KafkaJsonTidbOffsetUtils`（source 模块）：`BEGIN` 事务内 `SELECT TIDB_CURRENT_TSO()`（事务外返回 0，用户确认），`tso >> 18` 转物理毫秒，打 sentinel 返回 `KafkaJsonOffset(ms, MAX, MAX)`；失败返回 `null` 回退 Kafka 采样。
- `KafkaJsonDialect.displayCurrentOffset`：`scan.database.type=tidb` **且** `scan.message.event-time=es` 时走 TSO 路径，否则维持 Kafka 采样（TS 模式事件时间是发送时钟，与 TSO 不同域，不能混用）。
- **为什么比 Kafka 采样更对（重复/丢分析，2026-08-13）：** 基类 `AbstractScanFetchTask.execute` 在 `executeDataSnapshot` **前/后**各调一次 `displayCurrentOffset`（字节码 offset 62/186）。Kafka 采样的 H = 最新已发布消息的 es，被发布延迟拖后于库「now」→ `H_kafka < es ≤ 快照结束` 的事件若被模糊快照 chunk 读到，流阶段 `es > H` 不去重 → **重复**；topic 空时回 `INITIAL_OFFSET`(-1) → 回填 (L,H] 被跳过 + 流走 `scan.kafka.startup.mode`（默认 earliest 重读全量=重复；显式 latest 则快照期间未捕获且已发布的事件被 seek 越过=**丢**）。TSO 的 H ≥ 快照所有已捕获 commit_ts、且 topic 空也为非零 → 重复窗口塌缩、startup.mode 兜底不再被快照链路误触。**跨时钟（消息 Kafka 时间戳 < es）的丢数据窗口 TSO 治不了**——无界流读的 seek 仍按 Kafka 消息时间戳，属独立遗留项。
- 测试：`KafkaJsonTidbOffsetUtilsTest`（tso>>18 转换、连接失败回退 null）+ `KafkaJsonDialectTest`（TIDB+ES 走 TSO、MYSQL+ES/TIDB+TS 忽略 TSO）；`TiDBCdcChainITCase` 真实链路上实证。

---

## P3 — 健壮性 / DDL 扩展

### P3-1 时钟/时区一致性检测工具类

**背景：** EXACTLY_ONCE.md 已指出 exactly-once 的 loss 条件是 MySQL↔Kafka 时钟偏差；运行服务器时钟需单调递增且与数据库、Kafka 时钟一致。

**目标：** 一个工具类，检查并报告：数据库服务器时钟、Kafka 集群时钟（对 broker 时间取样）、运行环境（本机）时钟、各自时区是否一致；启动时打警告或 fail-fast（可配置）。

### P3-2 ALTER schema change 事件扩展

**现状：** ALTER 目前只 diff 出 Add/Drop/AlterType 列事件（借助前置 old 表 image）。

**目标：** 支持 `TRUNCATE TABLE`、`RENAME COLUMN`、`MODIFY COLUMN COMMENT` 等 DDL：
- TRUNCATE：目前会被解析成什么？（无列变化 → 可能空事件）语义上应清空数据；
- RENAME COLUMN：P0 已有列改名启发式（`RenameColumnEvent`），验证对真实 canal/TiCDC 的覆盖；
- MODIFY COLUMN COMMENT：仅注释变化是否产生事件、是否应产生。
- 需要为 pipeline 侧补对应用法（sink 是否消费、SchemaChangeEvent 类型选择）。

---

## P4 — Debezium（不急，用户明确低优先）

### P4-1 TiDB debezium 格式数据转换

**背景要点（来自调研）：**
- TiDB datetime 字面是几点，debezium 格式就是 UTC 多少——转换时设置时区为 UTC；
- `payload.ts_ms` 是 TiDB 生成的 UTC 时间戳，注意与 Kafka timestamp 比较是否有差距；
- TiCDC 会把 decimal 处理为 `float64` 而不是 `string`，转换器要兼容；
- 参考 doris connector 处理 mysql datetime/timestamp 的写法（`convertDateTime` / `convertTimestamp`，`ZonedDateTime` UTC → `timestampZoneId` 本地化）。
- 注意：TiDB 的 Timestamp 值在 debezium 是 `Instant.parse("2026-08-13T05:19:32Z")`（按系统时区转成标准日期）；Datetime 值是 epoch millis（把本地时间当 UTC 直接转）——两个行为不同，转换器要区分。

### P4-2 ticdc debezium schema change 支持

- TiCDC 8.x **不支持** debezium 的 schema change 事件，9 开始支持（9.0 暂未发布，dev 状态）。
- 测试路径：用 MySQL + Debezium（canal 格式已有）覆盖 schema change；等 ticdc 9.0 发布后再补 TiDB 侧。

---

## 建议执行顺序

1. **P0-1** 流式边界审计（先回答"END watermark 归属 + 越界剔除"，这是重复数据的核心）
2. **P0-2 / P0-3** 多 subtask 去重 + schema 注册串行化（合并成一个"并发正确性"工作项，因为根因很可能同源）
3. **P1-1** KafkaJsonValueConverter 类型测试（把转换行为锁死，后续 TiDB 特例在此扩展）
4. **P2-1** TiDB 消息额外处理（TIDB_WATERMARK 剔除 + DELETE old=null）——顺手确认 TiCDC 是否提供 `es`，决定 P2-2 是否升级
5. **P1-2 / P1-3** 类型工具类修复与整合
6. **P2-2** TSO → 时间戳（若 4 确认 `es` 缺失则提前）
7. **P3** 时钟工具类 + DDL 扩展
8. **P4** Debezium（等 ticdc 9.0 / 用户安排）

> 依赖注记：P0 三项互相纠缠（边界、去重、并发模型），建议按一个工作单元推进；P1-1 尽量与 P1-2 一起做（同为转换正确性）；P2-1 是 P2-2 的前置调研点。
