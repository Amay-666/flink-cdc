# jdbc-kafka-json-cdc 文档索引

> 分支：`feature/canal-rename-plan-a`（基于 `feature/3.2.1-custom`，flink-cdc 3.2.1）

本目录是连接器的全部文档。阅读方式遵循**渐进披露**：先读 [ARCHITECTURE.md](./ARCHITECTURE.md) 抓住全局，需要深度时再按需进入对应的子文档。

## 文档地图

| 文档 | 适合谁 | 讲什么 |
|---|---|---|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 所有人 | **总览**：连接器是什么、两个模块、总体数据流、关键设计决策、文档导航 |
| [ROADMAP.md](./ROADMAP.md) | 维护者 | **路线图**：按优先级的待办与已落地记录 |
| [DEV_NOTES.md](./DEV_NOTES.md) | 维护者 | **开发注意事项**：构建/测试命令、checkstyle 规则、类型与命名坑、设计约束、扩展指南 |
| [deep-dive/01-exactly-once.md](./deep-dive/01-exactly-once.md) | 调正确性的人 | **全量→增量切换的 exactly-once**：双水位算法、边界问题的两次修复 |
| [deep-dive/02-message-parsing.md](./deep-dive/02-message-parsing.md) | 加消息格式的人 | **消息解析层**：canal flatMessage 桥接、DDL 双解析器、Debezium 接入与真实链路 |
| [deep-dive/03-event-model.md](./deep-dive/03-event-model.md) | 加事件的人 | **事件模型与序列化栈**：事件类型树、自包含序列化、RENAME 事件 Plan A |
| [deep-dive/04-ddl-blocking.md](./deep-dive/04-ddl-blocking.md) | pipeline 维护者 | **DDL 阻塞协调机制**：自写 coordinator 如何阻塞-刷新-执行-放行，与 released 版逐文件对照 |
| [deep-dive/05-doris-sink.md](./deep-dive/05-doris-sink.md) | pipeline 维护者 | **Doris 写入与 DDL 执行**：StreamLoad 写入、DDL 生成、HTTP 交互 |
| [deep-dive/06-verification-infra.md](./deep-dive/06-verification-infra.md) | 测试维护者 | **正确性验证基建**：Workload/Ledger/对账、可观测性、场景矩阵 |

## 阅读建议

- **第一次接触**：读 `ARCHITECTURE.md` → 挑一个你关心的 `deep-dive/` 子文档。
- **排查重复/丢失**：`01-exactly-once.md`。
- **加一种消息格式**：`02-message-parsing.md`。
- **加一种 SchemaChangeEvent**：`03-event-model.md` + `DEV_NOTES.md` 的扩展指南。
- **改 pipeline 的 DDL 处理**：`04-ddl-blocking.md` + `05-doris-sink.md`。
- **搭新测试**：`06-verification-infra.md`。

## 目录约定

- 顶层三份文档（`ARCHITECTURE` / `ROADMAP` / `DEV_NOTES`）是导航入口，保持精简。
- `deep-dive/` 下是深入实现原理与代码解读的子文档，按需阅读。
- `coordinator-diff/` 是自写 coordinator 与 flink-cdc-runtime released 版的逐文件 diff 对照资料，
  供 [deep-dive/04-ddl-blocking.md](./deep-dive/04-ddl-blocking.md) 对照维护。
- 所有子文档可直接单独阅读，需要交叉引用时用链接。
