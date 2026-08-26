# 真实 CDC 链路计划：Debezium

> 承接 `DEBEZIUM_PLAN.md` §S4。S4 用的是**模拟** Debezium 信封；本节把真实链路补上：
> **MySQL → 真实 Debezium → Kafka**。与 Phase-11「模拟基线 + 真实链路」的成熟模式同构。

## 背景与已确认事实

1. **fork 内嵌 Debezium `1.9.8.Final`**（`pom.xml` `<debezium.version>`）。`KafkaJsonSourceInfoStructMakerTest`
   断言 source struct `version == "1.9.8.Final"`。→ 真实 producer 用 **`debezium/connect:1.9`**（1.9 系列最新），
   JSON 线格式与 parser 契约一致。
2. **`DebeziumMessageParser` 已兼容真实 Debezium 输出**：
   - `{schema, payload}` 包裹已 unwrap（`root.has("payload") ? root.get("payload") : root`）；
   - `Payload` 映射 `before/after/source/op/ts_ms/ddl/databaseName`；
   - tombstone（DELETE 后的 null 消息）由 `KafkaJsonStreamFetchTask` 的 `message == null` 兜底跳过。

## 一、`DebeziumCdcChainITCase`（MySQL + 真实 Debezium 1.9）✅ 已跑通

### infra：`DebeziumConnectContainer`
- 镜像 `debezium/connect:1.9`，网络别名 `debezium`，REST 端口 8083；
- env：`BOOTSTRAP_SERVERS=kafka:9092`、三个内部 topic（config/offset/status）、
  `JsonConverter`（schemas 开启 → 产出 `{schema,payload}` 包裹，正是要测的线格式）；
- `createConnector(name, mysqlHost, port, user, password, dbName, table, topicPrefix)`：
  `execInContainer` POST `/connectors`，connector.class=`io.debezium.connector.mysql.MySqlConnector`，
  `database.server.name=topicPrefix`（1.9 键名），`snapshot.mode=initial`（默认全量），
  `database.history.kafka.topic=dbhistory`；
- 主题名 = `{topicPrefix}.{dbName}.{table}`（1.9 的 server.name 前缀约定）。

### ITCase 时序（避免双快照竞态）
1. MySQL(8.0) + Kafka + Debezium 起；表已建好含 N 行；
2. 注册 connector → Debezium 全量快照 → **等 Kafka 主题攒够 N 条 `op:r` 记录**（确定性「Debezium 就绪」信号）；
3. 启动 source：JDBC 快照 N 行 → N 个 CreateEvent；流边界取快照后的 Kafka 位置（Debezium 快照记录在边界前，被排除）；
4. 库内执行 DML → Debezium 实时写 `op:c/u/d` → source 消费 M 条；
5. 断言：N 快照 CreateEvent + M 流事件（与 `expectedSnapshotEvents`/`expectedStreamEvents` 一致）。

## 二、PolarDB-X 链路 —— 已放弃（记录卡点）

`PolardbXChainITCase`（PolarDB-X → canal → Kafka）已尝试并**移除**。卡点在 PolarDB-X 官方镜像
`polardbx/polardb-x:v2.4.2_5.4.19` 的 CN（计算节点）分布式 DDL 引擎，**在 testcontainers 启动的容器里**
`CREATE DATABASE test_db` 的物理 DDL job 不落地：逻辑库建出、DN 物理库缺失，后续 `CREATE TABLE` 永远报误导性的
`ERROR 1046 No database selected`（完全限定的 `test_db.customers` 根本不该报这个），且一旦坏掉 60s/20 次重试（DROP+重建）
都不恢复。

### 排查结论（已逐项排除）
- 同样的 SQL、用户、容器内客户端，裸 `docker run` 起的容器 4/4 一次过（含自定义网络、远程用户、DDL 探针、真实
  Connector/J 空默认库连接复刻 `jdbcFailure()`）→ 非网络/端口/hostname/JDBC 连接/内存（Run 10 在 Kafka 启动前即失败）；
- 同一个 testcontainers 容器内，DDL 就绪探针（`__polardbx_ddl_probe` 库）建删成功 → 容器环境不阻止 DDL 引擎本身；
- 差异收窄到 testcontainers 传给 docker 的容器参数，未进一步定位（CN 内部 `/logs/tddl/ddl*.log` 未 dump 即放弃）。

结论：PolarDB-X 镜像 CN 的 DDL 行为在 testcontainers 环境不稳定，属被测库镜像缺陷，非连接器代码问题。若日后重启此链：
先在失败时 dump CN 的 `/logs/tddl/ddl*.log` 定位真实物理 DDL job 报错，再决定是绕过 testcontainers 还是换库名/镜像版本。

## 验证

```
mvn -o -pl .../flink-cdc-pipeline-connector-jdbc-kafka-json test -Dtest=DebeziumCdcChainITCase -DfailIfNoTests=false
```

全绿 → 提交 push（含审计）。任一真实链路暴露格式差异 → 如实报告，回计划层决策，不静默掩盖。
