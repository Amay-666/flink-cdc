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

# Doris Sink：StreamLoad 写入与 DDL 执行（全 HTTP，自包含）

> 本文讲连接器**完全自包含**的 Doris 写入：不引入 doris connector jar、不引入 mysql-jdbc，全部交互走 HTTP。
> 写数据用 StreamLoad PUT，执行 DDL 用 FE 的 HTTP 查询接口。
> 关联：[04-ddl-blocking.md](./04-ddl-blocking.md)（阻塞协议的数据落点）、[03-event-model.md](./03-event-model.md)（10 种事件的消费）。
> 代码位置：`sink/engine/doris/`（pipeline 模块）。

---

## 1. 为什么全 HTTP、为什么 OkHttp

| 约束 | 原因 |
|---|---|
| 不引入 doris connector jar | 用户拍板：连接器自包含，Doris 交互全走 HTTP，避免外部 jar 的版本/依赖风险 |
| 不引入 mysql-jdbc | DDL 执行走 FE HTTP 查询接口（`is_execute_sql_in_http=true`），不需要 JDBC |
| HTTP 客户端用 OkHttp 3.14.9 | 纯 Java、无 kotlin 依赖、自带连接池与重定向/重试控制，shade 进连接器 jar |

> 注：实现阶段把计划文件里「引入 doris jar 思路」的替代做成了唯一路径——两个 HTTP 端点见 §2。

---

## 2. 两个 HTTP 端点

| 端点 | 用途 | 说明 |
|---|---|---|
| `PUT /api/{db}/{table}/_stream_load` | StreamLoad 批量写 | 见 §3 |
| `POST /api/query/default_cluster/{db}`（body `{"stmt": ...}`） | DDL 执行 | 要求 FE 开 `is_execute_sql_in_http=true`（默认 false，见 §6 风险） |

> 早期计划文件写的 body 是 `{"sql":...}`，**实际实现是 `{"stmt":...}`**（与 Doris FE HTTP 查询接口一致）。

---

## 3. StreamLoad 写入（非 2PC）

### 3.1 类与职责

```
DorisSink implements Sink<Event>（非 TwoPhaseCommittingSink）
  └─ createWriter → DorisSinkWriter implements SinkWriter<Event>
       ├─ 本地 schema 视图（schemaMaps：TableId → Schema；rowConverters：TableId → DorisRowConverter）
       ├─ 每表 FIFO 缓冲（buffer：TableId → ArrayDeque<Map<String,Object>>，upsert/delete 同队列保序）
       └─ DorisHttpClient（OkHttp）
```

**为什么是"非 2PC"**：`Sink` 不是 `TwoPhaseCommittingSink` → 没有 precommit/commit 阶段，
**一次 StreamLoad PUT 就是一次提交**（at-least-once）。2PC 列为后续增强
（`two_phase_commit=true` + precommit/commit 两次 HTTP）。

### 3.2 触发 StreamLoad 的时机

1. **单表缓冲达到 `sink.buffer.size`**（每表阈值）；
2. **全局有界缓存**：总缓冲行数超过 `sink.buffer.max-buffered-rows` 时，**spill 最大的表队列**
   （防止跟踪很多小表时内存无界增长）；
3. **周期 flush 定时器**（`sink.flush.interval`，为 0/负则关闭）；
4. **每个 `FlushEvent`**：`DataSinkWriterOperator` 拦截 FlushEvent 直接调 `writer.flush()`（**强制 PUT**），
   这就是 [04-ddl-blocking.md](./04-ddl-blocking.md) 阻塞协议里"等所有并行度 flush 完"的数据落点；
5. **close()**：最终 PUT。

### 3.3 StreamLoad 请求细节（`DorisHttpClient.streamLoad`）

```
PUT {fe}/api/{db}/{table}/_stream_load
headers: label      = cdc_{db}_{table}_{uuid}
         format     = json
         strip_outer_array = true
         hidden_columns    = __DORIS_DELETE_SIGN__   ← delete 标记列（§3.4）
         Authorization = Basic(base64(user:pass))
         Expect       = 100-continue                ← FE StreamLoad handler 必需
body: JSON 数组（每元素是「列名 → JSON 值」的 Map）
```

**307 重定向两步走**：Doris 2.x 的 StreamLoad PUT 由 FE 答 `307`，`Location` 指向真正持有 tablet 的 BE。
OkHttp **不为 PUT 跟重定向**，所以客户端自己做两步：先问 FE（带 `Expect: 100-continue`），拿到 307 后
去掉 `Location` 里 FE 烘进去的 `user:pass@` 前缀，再对 BE 重发同一请求。

**失败重试是幂等的**：重试用**同一个 label**，Doris 按 label 去重。网络层失败按 `sink.max-retries` 重试
（`500ms * (attempt+1)` 退避）。

**成功判定**：HTTP 2xx 且响应体 `Status` 为 `Success` 或 `Label Already Exists`（后者等于"上次已成功"）。

### 3.4 DELETE 语义（Unique 模型 + 标记列）

- 每行都带 `__DORIS_DELETE_SIGN__` 标记：`true` = 按主键删除，`false` = upsert；
- 该列通过 `hidden_columns` header 声明，Doris 直接应用语义，**不需要 `merge_type`**；
- **建表不用 `enable_batch_delete_by_default`**（旧版批量删除开关，**Doris 2.x 已拒绝该属性**）——Unique 模型
  配合 `hidden_columns` 标记即可；
- DELETE 事件从**前像**（`event.before()`）构造行 + 标记 `true`；INSERT/UPDATE/REPLACE 都按 upsert
  （`event.after()` + 标记 `false`）；upsert 和 delete 共用一条队列，保证到达顺序。

> 早期计划文件里"建表属性 `enable_batch_delete_by_default=true`"的思路，实现阶段因 Doris 2.x 拒绝该属性而改为
> `hidden_columns` header。**以本实现为准。**

### 3.5 行转换

`KafkaJsonRowConverter`（抽象，`sink/converter/`）→ `DorisRowConverter`：
- `createExternalConverter(DataType)` 按 typeRoot 分派：数值/字符串原样、时间格式化（用 `pipelineZoneId`）、
  ARRAY/MAP/ROW → JSON 字符串；default 抛 `UnsupportedOperationException`；
- `convert(RecordData, Schema)` 产出「列名 → JSON 值」的 Map。

### 3.6 写入方如何演进 schema（`DorisSinkWriter.applySchemaChange`）

| 事件 | writer 本地行为 |
|---|---|
| `CreateTableEvent` | 注册 schema + converter |
| `RenameTableEvent` | **换 key**：remove 旧 tableId + put 新 tableId（后续数据带新 id）；缓冲队列一并迁移（rename DDL 前已 flush 过，队列应为空） |
| `DropTableEvent` | 丢弃该表缓冲 + schema + converter |
| `TruncateTableEvent` / 两个 Comment 事件 | **不动**（阻塞协议已先 flush；comment 不影响行转换） |
| 5 个标准 schema change | `SchemaUtils.applySchemaChangeEvent(current, event)` 演进 schema + 重建 converter |

> **坑**：`write(DataChangeEvent)` 时若 `schemaMaps` 里没有该表 → 抛 `IOException`
> （"CreateTableEvent must precede its data"）——**CreateTableEvent 必须早于数据到达**。

---

## 4. DDL 执行（MetadataApplier + DdlBuilder）

### 4.1 DorisMetadataApplier

运行在 **JobManager 的 schema-evolution coordinator 内**，`instanceof` 分派**全部 10 个事件**到
`DorisDdlBuilder`，生成一条或多条 SQL，经 `DorisHttpClient.executeSql` 依次执行：

```
applySchemaChange(event)
  ├─ buildSqls(event)：instanceof 分派 → DorisDdlBuilder.buildXxxSql(...)
  └─ 对每条 sql：client().executeSql(options.mapDatabase(tableId), sql)
```

- **全接受**（不做 `acceptsSchemaEvolutionType` 收窄）——匹配 coordinator 的透传 derivation；
- **HTTP client 懒创建**：`DorisMetadataApplier` 从 client 序列化到 JobManager，`OkHttpClient` 不可序列化，
  所以字段是 `transient`、首次用时 `synchronized` 建。

### 4.2 DorisDdlBuilder（事件 → Doris SQL）

- **CREATE TABLE**：`CREATE TABLE IF NOT EXISTS db.tbl (cols + COMMENT)`；有主键 → **UNIQUE KEY(pk)
  DISTRIBUTED BY HASH(pk) BUCKETS AUTO**；无主键 → **DUPLICATE KEY(首个物理列) DISTRIBUTED BY
  HASH(该列) BUCKETS AUTO**；metadata 列（非 physical）跳过。
- **类型映射**：所有时间戳 → `DATETIMEV2`（精度 clamp 到 `[0,6]`）；ARRAY/MAP/ROW → `STRING`
  （放行转换器产出的 JSON 文本）。
- **ALTER**：`ADD/DROP/RENAME/MODIFY COLUMN`（多列事件产多条单语句 DDL）；`ALTER TABLE old RENAME TO new`；
  `ALTER TABLE ... COMMENT '...'`；`ALTER TABLE ... MODIFY COLUMN col <type> COMMENT '...'`。
- **DROP**：`DROP TABLE IF EXISTS`（幂等）；**TRUNCATE**：`TRUNCATE TABLE`。

### 4.3 DDL 的重试策略（与 StreamLoad 不同）

```
executeSql：网络层失败按 max-retries 重试；应用层失败（HTTP code != 0）立即抛、不重试
```

**为什么 DDL 不重试**：DDL 不是幂等的（比如 CREATE TABLE 只生效一次），盲目重试会把失败的 DDL 再执行一遍。
所以只对网络层错误重试。

---

## 5. 代码清单

| 类 | 位置（pipeline 模块 `.../connectors/kafkajson/`） | 职责 |
|---|---|---|
| `DorisSink` | `sink/engine/doris/` | 非 2PC `Sink<Event>` |
| `DorisSinkWriter` | `sink/engine/doris/` | 缓冲 + 触发 StreamLoad + schema 演进 |
| `DorisHttpClient` | `sink/engine/doris/http/` | OkHttp：`streamLoad`（307 两步走 + label 幂等重试）+ `executeSql` |
| `DorisDdlBuilder` | `sink/engine/doris/ddl/` | 10 事件 → Doris SQL |
| `DorisMetadataApplier` | `sink/engine/doris/` | coordinator 内执行 DDL |
| `DorisDataSinkOptions` | `sink/engine/doris/` | Doris 配置（FENODES/USERNAME/PASSWORD/buffer/flush/前缀后缀/max-retries） |
| `DorisDataSinkDialect` | `sink/engine/doris/` | extends `KafkaJsonDataSinkDialect`，组装 sink/applier/converter |
| `DorisRowConverter` / `DorisWriteMetrics` | `sink/engine/doris/` | 行转换 / 写吞吐指标 |
| `KafkaJsonRowConverter` | `sink/converter/` | 行转换抽象基类 |
| `DorisSinkExample` | `example/` | 完整组装，可跑 MiniCluster |

---

## 6. 风险与边界

| 风险 | 应对 |
|---|---|
| FE 执行 DDL 需 `is_execute_sql_in_http=true`（默认 false） | 主走 HTTP；无法开启时后续切 MySQL 协议 JDBC（可选项，未实现） |
| StreamLoad 非 2PC = 至少一次 | 每 checkpoint/flush 一次 PUT + label 幂等；2PC 列后续增强 |
| Doris 2.x 拒绝 `enable_batch_delete_by_default` | 改用 `hidden_columns=__DORIS_DELETE_SIGN__` header |
| OkHttp 不可序列化（MetadataApplier 跨 client→JM 序列化） | `transient` + 懒初始化 |
| `is_execute_sql_in_http` 不可用时 DDL 全挂 | 阻塞协议会把作业卡死在 APPLYING → 需按上表切换 JDBC |

---

## 7. 验证

```bash
# 单测（Doris 用 JDK com.sun.net.httpserver.HttpServer 模拟，无 Docker）
mvn -q -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json \
  -am test -Dtest='DorisHttpClientTest,DorisSinkWriterTest,DorisDdlBuilderTest,DorisMetadataApplierTest,DorisRowConverterTest' \
  -DfailIfNoTests=false -Drat.skip=true -Dspotless.check.skip=true -Dmaven.javadoc.skip=true
```

要点：`MockDorisServer`（`test/.../http/`）模拟 FE/BE，断言 StreamLoad 请求路径/header（label、format=json、
hidden_columns、Authorization）/body、`Status!=Success` 时按 label 幂等重试、`executeSql` 请求路径与 body；
`DorisSinkWriterTest` 断言 DELETE 带 `__DORIS_DELETE_SIGN__`、schemaMaps 演进（标准 + 自定义事件）。
真实 Doris 容器端到端验证列后续阶段。
