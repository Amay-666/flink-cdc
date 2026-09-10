# jdbc-kafka-json 连接器代码评审与优化建议

> 评审范围：`flink-cdc-pipeline-connector-jdbc-kafka-json` 模块 `src/main` 全部 48 个 Java 文件。
> 评审依据：Apache Flink CDC 官方 `AGENTS.md` 编码规范（Spotless/Checkstyle、import 顺序、Java 11 基线、JUnit 5）与 Flink《Code Style and Quality Guide — Java》（日志/Preconditions 占位符、serialVersionUID、集合查找、Optional/@Nullable、异常与资源管理）。
> 对照基线：flink-cdc-runtime release 同名类、官方 mysql/doris pipeline connector、flink-cdc-common 标准事件与序列化器（结论均经实际读取对照，非臆测）。

---

## 一、总体结论

| 严重度 | 数量 | 说明 |
|---|---|---|
| **P0（数据丢失/崩溃，必须立即修）** | 3 | 2 条静默丢数路径 + 1 条序列化 NPE |
| **P1（正确性/健壮性/维护性，建议尽快修）** | ~25 | 线程安全、资源泄漏、DDL 语法错误、镜像引入的隐性缺陷 |
| **P2（规范/性能/可读性，择机清理）** | ~60 | import 顺序、canal 残留措辞、死代码、热路径开销 |

**四类结构性问题**（单点修不完，需专项治理）：

1. **镜像冗余约 70%**：schema 协调器 5 个类 ~1300 行中 ~900 行与 release 逐字相同（仅换类名）；OperatorID 哈希、RowConverter、序列化器等均为第 2~3 份拷贝。release 侧的 bug（SLF4J 吞栈、线程未命名、String.format 日志）被原样照抄，上游修复不会自动同步。
2. **canal 复制残留成片**：src/main 共 43 处 "Canal/canal" 措辞（javadoc、日志、异常消息、选项描述），误导维护者与用户。
3. **类型知识无单一事实来源**：CDC→Doris 类型映射在 DdlBuilder 与 RowConverter 各一份（已现 3 处漂移）；Debezium→CDC 列转换 `toColumn` 双实现且语义漂移（默认值丢/保不一致）。
4. **example 包位于 src/main**：硬编码密码进生产 jar，且被测试反向依赖。

---

## 二、P0 问题（3 个）

### P0-1 DorisSinkWriter 周期 flush 线程不安全 —— 可能 HashMap 损坏/丢行
`DorisSinkWriter.java:279-299`：定时回调在 `SystemProcessingTimeService` 的独立线程执行，与 mailbox 线程上的 `write()/flush()/close()` **无锁并发**访问 `buffer`(HashMap)、`schemaMaps`、`rowConverters`、`bufferedRows`；`close()` 中的 flush 也会与在飞回调并发。
**修复**：回调内改用 `initContext.getMailboxExecutor().execute(...)` 把 flush 投回 mailbox 线程后再重注册定时器；或对所有缓冲状态加锁。

### P0-2 DorisHttpClient "Label Already Exists" 一律视为成功 —— 静默丢数
`DorisHttpClient.java:260-261`：首次尝试已被 Doris 判失败（如行过滤超限）后重试同 label，Doris 返回 Label Already Exists，当前代码当成功返回——整批数据静默丢失且任务不失败。叠加 `flushTable`（DorisSinkWriter:244-256）在 StreamLoad 成功前就 `buffer.remove` 并 drain，构成完整丢数链路。
**修复**：解析响应中的 `Existing Job Status`（或调 get_load_state）确认此前事务确已成功；Doris 明确失败（Fail 状态）的重试应换新 label，仅结果未知的网络类失败才复用同 label。

### P0-3 四个 per-event 序列化器对 @Nullable sql 直接 NPE
`KafkaJsonDropTableEventSerializer.java:85`、`KafkaJsonRenameTableEventSerializer.java:92`、`KafkaJsonTruncateTableEventSerializer.java:87`、`KafkaJsonAlterTableCommentEventSerializer.java:88`：对声明为 `@Nullable` 的 `getSql()` 使用 Flink base `StringSerializer`（底层 `writeUTF`），null 直接 NPE、超 64KB 抛 UTFDataFormatException。生产路径 `KafkaJsonEventDeserializer` 的 sql 取自 `getString(DDL_STATEMENTS)`，可为 null；事件进入 shuffle/checkpoint 序列化即崩。现有 round-trip 测试全部传非 null sql，恰好掩盖。
**修复**：改用 `NullableSerializerWrapper<StringSerializer>` 或 runtime `StringSerializer`（StringValue 写法，无 64KB 限制），并补 null-sql 往返单测。

---

## 三、按包逐文件明细

### 3.1 source 包（7 文件）

#### factory/KafkaJsonDataSourceFactory.java
- [规范|P2] 210-213: LOG.info 字符串拼接构造格式串，且消息写死 "Canal data source" → 改占位符并更正连接器名。
- [可读性|P2] 200-205: 用户可见异常消息 "The canal pipeline connector supports..." → canal 残留，改措辞。
- [正确性|P2] 292-293: `SCAN_STARTUP_TIMESTAMP_MILLIS` 缺省时 `StartupOptions.timestamp(null)` NPE 无上下文 → 显式存在性校验。
- [正确性|P2] 197-199: `table.substring(0, table.indexOf('.'))` 依赖含 '.'，否则裸 StringIndexOutOfBoundsException → 先校验或用 TableId 解析。
- [规范|P2] 50-76 等: 29 行超 100 列 → 跑 `mvn spotless:apply`。
- [冗余|P2] 274-407: `getStartupOptions`/`getServerTimeZone`/`validate*` 与 MySqlDataSourceFactory 逐字镜像 → 下沉公共工厂工具类。

#### source/KafkaJsonEventDeserializer.java
- [规范|P1] 20: `org.apache.flink.api.*` import 排在 `org.apache.flink.cdc.*` 之前 → 调整顺序。
- [正确性|P2] 137-143: `value.getStruct("source")` 未判空，且硬编码 "source" → 判空抛带 record 信息的异常，用 `AbstractSourceInfo` 常量。
- [可读性|P2] 197-278: `x == null ? Schema.newBuilder().build() : toSchema(x)` 重复 3 次 → 提取辅助方法；`io.debezium.relational.Table` 全限定名内联无重名冲突 → 正常 import。
- [规范|P2] 133: schema name 用 `equalsIgnoreCase`（Debezium 全名区分大小写）→ 改 `equals`（继承自 MySQL 版，可选）。

#### source/KafkaJsonDataSourceOptions.java
- [冗余|P1] 31-282: 约 20 个通用选项与 MySqlDataSourceOptions 逐项镜像（描述文本原样复制）→ 下沉公共模块。
- [冗余|P2] 37-41: `PORT`（key="port"/3306）与依赖模块 `KafkaJsonSourceOptions.CANAL_MYSQL_PORT` 跨模块重复定义 → 引用同一常量。
- [规范|P2] 115-116: "startup mode for Canal CDC consumer" → canal 残留描述，更正。
- [规范|P2] 48-282: 约 35 行超 100 列 → 拆行。

#### source/KafkaJsonMetadataAccessor.java
- [可读性|P1] 42-63: javadoc 与异常消息均为 "Canal does not support..." → 改 jdbc-kafka-json 措辞。

#### source/KafkaJsonDataSource.java
- [可读性|P1] 30: javadoc "A DataSource for canal cdc connector" → 更正。
- [冗余|P2] 39: 构造器 `configFactory.create(0)`——同一路径第 2 次创建等价配置对象（见共性 3.7-③）。

#### source/KafkaJsonEventSource.java
- [冗余|P2] 52: `new KafkaJsonDialect(configFactory.create(0))` 第 3 次 create(0) → 复用 DataSource 已持有的 sourceConfig。

#### source/reader/KafkaJsonPipelineRecordEmitter.java
- [冗余|P1] 51: `LOG` 声明后全类未使用（重构遗留死字段）→ 删除。
- [规范|P2] 20: import 顺序问题（同上）。
- [正确性|P2] 122: `sourceConfig.getDatabaseList().get(0)` 空列表裸 IndexOutOfBoundsException → 判空抛带上下文异常。
- [可读性|P2] 58-59: 字段注释与实际行为不符（INITIAL 模式 snapshot→stream 转换同样置位）→ 更正。
- [性能|P2] 87-90: 每表首次低水位事件新建 JDBC 连接 → 可在 snapshot 生命周期内复用。

### 3.2 sink 核心 / partitioning / converter（6 文件）

#### sink/KafkaJsonDataSinkBuilder.java
- [规范|P1] 20: import 顺序（`sink2.Sink` 先于 cdc）→ 调整。
- [冗余/正确性|P1] 38, 173-181: 手工复刻 Flink StreamGraphHasherV2 murmur3 哈希，本仓库已有第 2 份（paimon 模块 OperatorIDGenerator）→ 抽公共工具类，或补与 Flink 实际 OperatorID 对齐的锚定单测；Flink 升级改哈希算法时 partition/writer 将静默寻址不到 coordinator。
- [可读性|P2] 88: javadoc "...and the Doris writer" 复制残留 → 改为方言无关表述。

#### sink/KafkaJsonDataSinkOptions.java
- [冗余|P2] 84-98: 4 个 public getter 无外部调用方 → 收窄 private。
- [封装|P2] 80-82: `getConfig()` 暴露可变 Configuration → 返回副本或收窄。
- [线程安全|P2] 71-74: volatile 映射的中途替换无原子性保障 → 注释显式标注非线程安全。

#### sink/dialect/KafkaJsonDataSinkDialect.java
- [规范|P1] 20: import 顺序。
- [冗余/死代码|P1] 70: `abstract createRowConverter(Schema, ZoneId)` 全模块无调用方（DorisSinkWriter 直接 new DorisRowConverter 绕过抽象入口）→ 删除或反向收敛为统一经 dialect 创建。
- [可读性|P2] 75: 英文 javadoc 混入中文「表原始名称 + 主键 id」→ 统一英文。

#### sink/partitioning/KafkaJsonPartitioningEventTypeInfo.java
- [正确性|P1] 33-40: 未覆盖 `equals/hashCode/canEqual`，而父类 `PartitioningEventTypeInfo` 的 equals 误写为 `instanceof PartitioningEvent`（恒 false），两个相同实例永不相等；兄弟类 `KafkaJsonEventTypeInfo` 已做防御性覆盖 → 按相同模式补齐。
- [健壮性|P2] 37-40: 仅覆盖 `createSerializer(ExecutionConfig)`；master 已新增 `createSerializer(SerializerConfig)` 重载 → 加注释标记，升级时同步补覆盖。
- [规范|P1] 20-21: import 顺序。

#### sink/partitioning/KafkaJsonPrePartitionOperator.java
- [健壮性/正确性|P1] 127-163: 热路径 `cachedHashFunctions.get(tableId)` 未解包 Guava 的 `ExecutionException/UncheckedExecutionException`，表名上下文丢失；schema 缺失直接 fail-fast 无重试（历史上 "schema 未注册" 挂作业的路径）→ 捕获解包重抛（附 tableId/subtask），对"尚未注册"瞬时态考虑有限重试或 refresh。另 `expireAfterAccess(1天)` 过期后低频表会周期性在热路径同步 RPC。
- [性能|P2] 113: 每 DDL 同步 RPC → 可考虑异步 refresh。
- [冗余|P2] 60-61, 181-185: `implements Serializable` 与空 `snapshotState` 均为镜像冗余 → 删除留注释。
- [冗余|P2] 80-185: `recordsOutCounter` 与 Flink 内建 `numRecordsOut` 重复 → 删除。

#### sink/converter/KafkaJsonRowConverter.java
- [正确性|P1] 124-126: `rebuild()` volatile 字段发布顺序错误（先写 schema 后写 converters），并发读可见"新 schema+旧 converters"错配 → 先写 converters 最后发布 schema，或封装为不可变 holder。
- [性能/冗余|P1] 74-77: 热路径每行 `schema.equals(this.schema)` O(列数)；而唯一调用方 DorisSinkWriter 每次 DDL 已 new 新 converter，懒重建路径生产中永不触发（双层机制并存）→ 去掉 convert 的 schema 参数（构造时绑定），或加 `!=` 引用比较短路。
- [性能/死代码|P2] 109-123: rebuild 中逐名 `schema.getColumn(name)` 线性查找整体 O(n²)，且 `orElseThrow` 分支不可达 → 直接遍历 `getColumns()`，删死防御。

### 3.3 schema 协调器 + utils（9 文件）

#### sink/schema/KafkaJsonSchemaOperatorFactory.java
- [正确性|P1] 37-57: EXCEPTION 模式下 `RenameTableEvent/TruncateTableEvent.getType()` 谎报 `CREATE_TABLE`（各自 javadoc 自认 placeholder）会**绕过** SchemaOperator 的拒绝检查被放行执行；`DropTable/Alter*Comment.getType()` 直接抛异常 → 建议在 Builder/工厂层对 `schemaChangeBehavior == EXCEPTION` fail-fast 拒绝。
- [镜像冗余|P2] 全文件: release `SchemaOperatorFactory` 去 routing 的逐行翻版 → provider 可注入后可整体删除。

#### sink/schema/coordinator/KafkaJsonSchemaRegistry.java
- [镜像冗余|P1] 132-384: 与 release 约 80% 逐字重复（handleGetEvolved/OriginalSchemaRequest 等最易被基类吸收）→ 见共性治理建议。
- [序列化|P1] 186-208: checkpoint 状态与 release 线格式**不互兼容**（release 恢复时会继续读 derivation-mapping 尾巴 → EOFException）→ javadoc 显式声明；若有"同 uid 从 release 平滑迁移"需求必须补写空 mapping。
- [序列化|P2] 283-305: `resetToCheckpoint` 只认 version 2，但 Serializer 保留 0/1 死分支 → 统一删掉。
- [日志|P2] 313-318: `LOG.error(String.format(...))` → 占位符。

#### sink/schema/coordinator/KafkaJsonSchemaRegistryProvider.java
- [镜像冗余|P1] 68-103: 内部 ThreadFactory 57 行与 release 逐字相同（因 package-private 无法复用才被迫复制）→ 建议在 runtime 提为 public 工具类收敛。
- [规范|P2] 31: 缺 `@Internal` 注解（release 版有）。

#### sink/schema/coordinator/KafkaJsonSchemaRegistryRequestHandler.java
- [镜像冗余|P1] 132-218: `handleSchemaChangeRequest` 86 行状态机与 release 逐字相同，全文件重复度 ~70% → release handler 参数化（注入 SchemaManager 抽象+策略+钩子），本类退化为 3 个策略方法。
- [正确性|P2] 253-257: `LOG.error("... Caused by: {}", ..., t)` 3 占位符吞掉异常栈（release 同款 bug 被照抄）→ 改 2 个占位符 + t 作独立最后参数。
- [死代码|P2] 183-197: `derivedSchemaChangeEvents.isEmpty()` 分支永不为真（derivation 恒返回 singletonList），`SchemaChangeResponse.ignored()` 不可达；注释仍在描述 release 的 LENIENT/route 行为 → 删死分支与失实注释。
- [冗余|P2] 84-90: `currentIgnoredSchemaChanges` 只写不读 → 上报或删字段。
- [资源|P2] 116, 341-345: 线程池未命名（pool-N-thread-1）；`close()` 中 `if (schemaChangeThreadPool != null)` 对 final 字段永真 → 删。

#### sink/schema/coordinator/KafkaJsonSchemaManager.java
- [正确性|P1] 131-196, 327-332, 421-424: **drop→recreate 同名表静默 schema 漂移**：DropTableEvent 保留旧 entry（有意），但同名表随后的 CreateTableEvent 会被 presence 判断误判为 duplicate 被吞（新表结构永不下发 sink）；rename 回退（A→B→A）同理被误吞。这是镜像时只推演单事件、未推演事件序列的连带缺陷 → 支持"删除后重建"的 entry 覆盖。
- [镜像冗余|P1] 447-556: Serializer ~110 行逐字相同（连版本历史注释都照抄，本 connector 从未存在过 0/1 状态）→ 抽公共三元组序列化器。
- [正确性|P2] 206-224, 373-399: AlterColumnCommentEvent 引用不存在列时静默忽略、schema 原样 bump 版本 → checkArgument 或 warn。
- [冗余|P2] 100-104, 291-307: 无参构造器标 `@VisibleForTesting`；equals/hashCode 忽略 behavior 字段（照抄 release）。

#### sink/schema/coordinator/KafkaJsonSchemaDerivation.java
- [文档失实|P1] 29-36: javadoc 声称 "route rules are rejected up front"，但全模块 grep 不到任何拒绝逻辑，实际是**静默忽略** → 要么在 Builder 入口加 "routes 非空即抛异常" 兑现注释，要么改写注释。
- [冗余|P2] 38-43: 无状态透传类 → 可内联进 handler；若为将来 route 预留接缝，javadoc 明示取舍。

#### sink/schema/coordinator/OldSchemaAwareMetadataApplier.java
- [设计|P2] 18-48: 能力接口放在 coordinator 包但实现方是 sink 侧 applier，包位置倒置 → 挪 sink 包；若后续 rename/drop 也需 old schema，建议直接设计为 `applySchemaChange(event, SchemaChangeContext)` 上下文形态。

#### utils/KafkaJsonSchemaUtils.java
- [重复|P1] 141-146: `toColumn` 与 `SchemaChangeUtil.toColumn` 概念重复但**语义漂移**（此处丢 defaultValueExpression，彼处保留）→ 同一列经 source 建表/增量 diff 两条链路转换结果不同，统一为保留默认值的版本。
- [硬编码|P2] 94-126: `listDatabases/listTables` 硬编码 MySQL 方言（SHOW DATABASES/反引号）却放通用 utils；`quote()` 不转义含反引号的库名 → 下沉 dialect 层并做转义。
- [异常|P2] 56-83: 三处 `RuntimeException("...: " + e.getMessage())` 拼接丢结构 → 统一。
- [结构|P2] 49-92: 类同时承担 JDBC 元数据访问与类型转换两类职责，`getTableSchema` 每次 new Dialect → 拆分。

#### utils/SchemaChangeUtil.java
- [职责|P1] 229-243: `toColumn` 与核心职责（diff 推断）无关且与上述重复 → 挪走并统一。
- [性能|P2] 114-136: `columnMatchCost` 每次比较重复执行 Debezium 类型解析，minCost 与 traceback 各算一遍 O(n·m) 次 → 预计算数组。宽表收益明显。
- [健壮性|P2] 78-107: `minCost` 递归深度 n+m，数千列表有 StackOverflow 风险 → 改自底向上迭代 DP。
- [命名|P2] 43: 与 KafkaJsonSchemaUtils 边界模糊 → 改名 `MinimalSchemaChangeInferrer` 自解释。

### 3.4 doris engine（10 文件）

#### DorisSink.java
- [规范|P1] 20-22: import 顺序 → 调整。其余干净。

#### DorisSinkWriter.java
- [正确性|P0] 279-299: 定时 flush 线程不安全（见 P0-1）。
- [资源|P1] 134-141: `close()` 中 flush 抛异常则 `httpClient.close()` 不执行，OkHttp 线程/连接池泄漏 → try/finally。
- [正确性|P1] 143-163: 行转换含非物理（metadata）列，而 DDL 侧只建物理列 → schema 含 metadata 列时 StreamLoad 报 unknown column 整批失败 → 两侧同样按 `Column::isPhysical` 过滤。
- [异常|P1] 288-298: 定时回调把 IOException 包 RuntimeException 抛定时器线程，失败路径零日志（LOG 死字段），若未触发 failJob 则丢行 → 失败路径记日志并确保异常传播到任务失败。
- [冗余|P2] 73: LOG 声明未使用 → 删除或补关键路径日志。
- [正确性|P2] 213-219: 未知表的 schema change 事件被静默忽略 → 至少 LOG.warn 暴露乱序。
- [性能|P2] 165-168: `computeIfAbsent` 后又 `get` 二次查找 → 复用返回引用。
- [性能|P2] 301-312: `newLabel` 每次用 `String.replaceAll` 重复编译正则 → 预编译 static Pattern。
- [规范|P1] 20-22: import 顺序。

#### DorisMetadataApplier.java
- [异常|P1] 96-98: `new SchemaEvolveException(event, e.getMessage(), null)` 丢弃原始 cause，排障无堆栈（官方同款缺陷被镜像）→ 第三参传 e。
- [规范|P1] 37: 该行 103 字符超宽 → 折行。
- [规范|P2] 62: 全组唯一缺 serialVersionUID 的 Serializable 类 → 补。
- [可读性|P2] 101-124: 10 连 instanceof 分发 → switch/映射表收敛（低优先）。

#### DorisDataSinkDialect.java
- [冗余|P1] 67-70: `createRowConverter` 无调用方（writer 自建转换器）→ 死代码删除（与 KafkaJsonDataSinkDialect 的抽象入口二选一收敛）。
- [规范|P1] 20-25: import 顺序。
- [可读性|P2] 34-35, 68-69: javadoc 残缺；`pipelineZoneId` 参数遮蔽同名字段，两处时区来源并存 → 重命名参数。
- [正确性|P2] 48-50: 无 ZoneId 构造器静默 `systemDefault()`，JM/TM 时区不一致时行为不确定 → 显式传入或 warn。

#### DorisDataSinkOptions.java
- [可读性|P2] 73-74: `sink.max-retries` 实为"总尝试次数"，命名不符 → 改名或文档明确。
- [正确性|P2] 41-109: `fenodes`/`username` 无默认值且未校验必填，null 时在 HttpClient 构造器 NPE 难定位 → Preconditions（%s 占位）前置校验。
- [可读性|P2] 128-130: `getStreamLoadProperties()` 返回 null，调用方各自判空 → 统一空 Map。

#### DorisRowConverter.java
- [正确性|P1] 110-113: 支持 TIME 渲染但 DdlBuilder 无 TIME 分支 → 含 TIME 列的表建表直接 UnsupportedOperationException，两侧能力矩阵不一致 → DDL 补 TIME 映射（Doris 2.1+ 支持）或两侧一致报不支持。
- [冗余|P2] 80-146, 187-235: `createExternalConverter` 与 `convertValue` 两套平行类型分派，日期/时间戳格式化重复 ~90 行 → 抽公共渲染函数。

#### DorisWriteMetrics.java
- [可读性|P2] 44-94: 每 record*/set* 方法判空样板 → Null object 模式收敛。整体干净（AtomicInteger 可见性处理正确）。

#### http/DorisHttpClient.java
- [正确性|P0] 260-261: Label Already Exists 静默丢数（见 P0-2）。
- [正确性|P1] 154-290: 重试循环外构建 Request、`feEndpoint()` 只调用一次 → 重试永远打同一 FE，无法故障转移 → 每次尝试重建请求轮询下一个 FE。
- [正确性|P2] 254-302: 401/403/400 等不可重试错误进入重试循环，错误凭据时每个 DDL 白等 3×退避 → 4xx（除 429/408）立即失败。
- [正确性|P2] 226-236: `streamLoadProperties` 允许覆盖 `label`/`hidden_columns` 协议关键头 → 误配静默破坏 DELETE 语义与幂等 → 纳入框架管理头禁止覆盖。
- [资源|P2] 109: fenodes 未校验（空串/逗号后空格产生非法 host）→ trim + 过滤空项 + 校验非空。
- [异常|P2] 349-355: `sleepQuietly` 恢复中断位后仍继续重试，取消延迟退出 → InterruptedException 时中止并抛出。
- [可读性|P2] 120-124, 351: 超时 30/60/60s、退避 500ms×attempt 硬编码无抖动 → 常量化或接入 options。
- [冗余|P2] 160-224: `streamLoad` 与 `performStreamLoad` 两段几乎相同的重试循环 → 抽通用重试模板。

#### ddl/DorisDdlBuilder.java
- [正确性|P1] 276-283: `ALTER TABLE db.t COMMENT '...'` 非 Doris 合法语法（应为 `ALTER TABLE ... MODIFY COMMENT "..."`）→ 每个 AlterTableCommentEvent 都以语法错误失败并中断作业 → 改 MODIFY COMMENT（列注释的 MODIFY COLUMN ... COMMENT 写法是对的）。
- [正确性|P1] 306-341: `convertDataType` 缺 `TIME_WITHOUT_TIME_ZONE`（与 RowConverter 漂移）→ 补。
- [正确性|P2] 104-144: 非物理列过滤后 `UNIQUE KEY(...)` 仍引用完整 primaryKeys → 主键含非物理列时生成引用不存在列的 DDL → 两处用同一列集。
- [正确性|P2] 533-547: `escapeSql` 不处理反斜杠/换行；`quoteProperty` 不转义内嵌双引号 → 注释含特殊字符时 DDL 失败。
- [正确性|P2] 170-185: `buildAddColumnSql` 丢弃 FIRST/AFTER 位置信息 → 注释声明或支持。
- [可读性|P2] 105: `HashSet<String> pkSet = new LinkedHashSet<>()` 声明/实例类型不符且顺序无意义 → `Set<String> pkSet = new HashSet<>(...)`。
- [可读性|P2] 100-158: `buildCreateTableSql` ~58 行 → 拆三个私有方法。
- [冗余|P2] 553-557: 自实现 `singleton()` → `Collections.singletonList`。

### 3.5 event / serializer / example（16 文件）

#### event 包（5 文件，共性：serialVersionUID/final/equals/hashCode 齐全，模式符合 common 基线）
- AlterTableCommentEvent.java: [文档|P2] 55-62 javadoc "the truncated table" 复制残留；方法顺序与包内不统一。
- AlterColumnCommentEvent.java: [风格|P2] 86 方法顺序/缺 javadoc（其余无问题，与 common RenameColumnEvent 完全同构）。
- DropTableEvent.java: [文档|P2] 51-57 "the truncated table" 残留。
- TruncateTableEvent.java: [文档-实现矛盾|P1] 38-41 vs 79-83 javadoc 声称 getType() 返回 CREATE_TABLE 占位，实现抛 UnsupportedOperationException → 二选一；[文档|P2] "canal serialization stack" 措辞。
- RenameTableEvent.java: [文档-实现矛盾|P1] 38-41 vs 91-95 同上；[文档|P2] canal 措辞。

#### serializer 包（9 文件）
- KafkaJsonEventTypeInfo.java: [文档|P2] 30-36 canal 措辞；equals 对父类不对称的局限文档未如实描述。
- KafkaJsonEventSerializer.java: [冗余|P2] runtime EventSerializer 逐行拷贝（上游 final，务实选择）→ 类头注明镜像同步义务；[拼写|P2] 148-152 枚举 `SCHEME_CHANGE_EVENT` 应为 SCHEMA（序数不变可修名）；[微瑕|P2] `+ from.toString()` 冗余。
- KafkaJsonSchemaChangeEventSerializer.java: [可维护性|P1] 92-246 同一 10 路分发在 copy/serialize/deserialize 三处平行展开，新增事件需同步 5 处 → 抽 `Map<tag, TypeSerializer>` 注册表；[文档-兼容性|P2] 55-56 前 5 个 tag 顺序与 runtime 枚举不同、判别字节不兼容，javadoc "reuse byte formats" 表述易误导 → 修正文档或对齐顺序。
- KafkaJsonPartitioningEventSerializer.java: [风格|P2] 39 缺 final、serialVersionUID、@Internal（包内 8 个兄弟均齐全）→ 补齐统一。
- KafkaJsonDropTableEventSerializer.java: [缺陷|P0] 85 null-sql NPE（见 P0-3）；[文档|P2] 45 INSTANCE javadoc 错写成 RenameTableEventSerializer；canal 措辞。
- KafkaJsonRenameTableEventSerializer.java: [缺陷|P0] 92 同上；canal 措辞。
- KafkaJsonTruncateTableEventSerializer.java: [缺陷|P0] 87 同上（且 2 参构造器显式默认 sql=null，入口更近）；与 DropTable 序列化器结构完全同构可合并。
- KafkaJsonAlterTableCommentEventSerializer.java: [缺陷|P0] 88 同上；[文档|P2] 46 INSTANCE javadoc 类名错误；canal 措辞。
- KafkaJsonAlterColumnCommentEventSerializer.java: [风格|P2] 24-25 用 runtime StringSerializer（StringValue）而 4 个兄弟用 Flink base StringSerializer（writeUTF），同包混用两个同名类 → 统一（本类的 MapSerializer null 处理是 5 个中唯一正确的）。

#### example 包（2 文件）
- DorisSinkExample.java: [安全|P1] 65-79 硬编码密码 "123456"（MySQL/Doris 各一处）与 host/port/topic → 参数化/占位符；[结构|P1] 位于 src/main 却被 `KafkaJsonDdlBlockingITCase` 复用作测试夹具，进生产 jar → 下沉 src/test 或独立 examples 模块；[死代码|P2] 54 `CANAL_SOURCE_IDENTIFIER` 全模块零引用且与工厂常量重复 → 删除。
- KafkaJsonRenameStateOperator.java: [实际用途|P1] 恒等直通演示算子（DataChangeEvent 分支仅 LOG.debug），仅测试引用 → 与上条一并下沉 src/test；[风格|P2] Logger 全限定名内联声明 → 改 import。

---

## 四、共性结构性问题与治理建议

### ① 镜像冗余 ~70%（最大长期维护负债）
schema 协调器 5 类 + SchemaOperatorFactory + PartitioningEventSerializer + EventSerializer + OperatorID 哈希，合计 ~1500 行与 release/其他模块逐字重复。release 侧已知 3 处反模式（SLF4J 吞栈、线程池未命名、String.format 日志）全部被照抄。
**治理路径**：
1. 短期：每个镜像类头部注明"镜像自 release X.Y.Z + 差异清单"，为镜像点补锚定单测；
2. 中期：在 flink-cdc-runtime 把 `SchemaRegistryRequestHandler`/`SchemaRegistry` 参数化（注入 SchemaManager 抽象接口 + 事件过滤策略 + applier 钩子），`SchemaManager.Serializer` 抽公共静态方法，ThreadFactory 提 public——connector 侧可缩到 ~300 行纯差异；
3. 根治：推动 release 的 `SchemaChangeEventType` 枚举扩展 5 个自定义事件值，届时整个 instanceof-vs-getType() 镜像层可退役。

### ② canal 复制残留（43 处）
javadoc/日志/异常消息/选项描述中 "Canal/canal" 成片（Factory、Options、MetadataAccessor、DataSource、Deserializer、Truncate/Rename 事件、5 个序列化器、Example）。一次性专项清理。

### ③ 类型知识无单一事实来源
- CDC→Doris：DdlBuilder.convertDataType 与 RowConverter 各一份 → 抽 `DorisTypeMapping` 共享，消除 TIME 缺口/非物理列/注释语法 3 处漂移；
- Debezium→CDC：`KafkaJsonSchemaUtils.toColumn` 与 `SchemaChangeUtil.toColumn` 语义漂移（默认值丢/保）→ 统一保留默认值版本。

### ④ 分发逻辑无注册表
`KafkaJsonSchemaChangeEventSerializer` 的 tag 枚举 + copy 10 路 if-else + serialize 10 路 + deserialize switch 三处平行，新增事件需同步 5 处且漏注册只在运行时暴露 → `Map<tag, serializer>` 注册表收敛。

### ⑤ 事件序列正确性盲区
镜像时只推演单事件路径：drop→recreate 被误判 duplicate（P1）、EXCEPTION 模式 getType() 谎报绕过拒绝检查（P1）、rename 回退误吞。建议补"事件序列"维度的单测（drop→create、rename A→B→A、EXCEPTION 模式 × 5 自定义事件）。

### ⑥ 规范批量欠账
- import 顺序（`org.apache.flink.api.*` 先于 `org.apache.flink.cdc.*`）：至少 8 个文件（Deserializer、RecordEmitter、Builder、Dialect、PartitioningEventTypeInfo、DorisSink、DorisSinkWriter、DorisDataSinkDialect）→ 一次修完；
- ~70 行超 100 列、多处日志/异常字符串拼接、2 处缺 serialVersionUID、DorisMetadataApplier 超宽行 → 统一跑 `mvn spotless:apply` + 人工过一遍。

---

## 五、建议修复顺序

| 批次 | 内容 | 验证手段 |
|---|---|---|
| **第一批（P0 + 丢数/挂作业）** | DorisSinkWriter 定时 flush 线程安全 + close 泄漏；DorisHttpClient Label Already Exists 与 FE 故障转移；4 个序列化器 null-sql；DorisDdlBuilder 表注释语法 | null-sql round-trip 单测；并发 flush 压测；Doris 集成测试含失败-重试场景 |
| **第二批（P1 正确性）** | drop→recreate 误吞；EXCEPTION 模式 fail-fast；metadata 列过滤；TIME 类型映射；KafkaJsonRowConverter 发布顺序与每行 equals；PartitioningEventTypeInfo equals；PrePartitionOperator 异常解包；SchemaEvolveException 丢 cause；checkpoint 兼容性声明 | 事件序列单测；含 TIME/metadata 列的建表 ITCase |
| **第三批（结构性重构）** | 镜像参数化收敛（runtime 侧改造）；类型映射/列转换统一；序列化器注册表化；example 下沉 src/test；canal 残留清理 | 全量回归 + 与 release 行为 diff 的锚定测试 |
| **第四批（规范清理）** | import 顺序、spotless、死代码（LOG 字段、ignored 集合、createRowConverter、CANAL_SOURCE_IDENTIFIER、0/1 序列化死分支）、性能小项（预编译正则、computeIfAbsent 复用、columnMatchCost 预计算） | `mvn spotless:check` + checkstyle |

---

*评审方法：5 组并行逐文件评审（source 7 / sink 核心 6 / schema+utils 9 / doris 10 / event+serializer+example 16），全部文件完整读取，镜像与重复结论均经与 release 版实际对照确认。*
