-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ============================================================================
-- Doris 表名对调（rename swap）实验矩阵
-- ----------------------------------------------------------------------------
-- 目的：决定 DorisDdlBuilder 在遇到「pairs 在名字上成环」（如 RENAME a TO b,
--       b TO a）时该生成什么 SQL：REPLACE WITH TABLE ... swap=true，还是
--       临时名三步走。
-- 环境：apache/doris:fe-2.1.8 + be-2.1.8（单 BE，见下方 replication 属性）
-- 执行：docker exec doris-fe mysql -h127.0.0.1 -P9030 -uroot < 本文件
--       以及 HTTP 通道 curl（见文末 E5）
--
-- 实测结论（2026-09-15，全部在本文件记录的语句上跑通）：
--   E1/E2 ✅ 结构完全不同的两表（列数/类型/主键/key 模型都不同，UNIQUE ↔
--            DUPLICATE）可以 REPLACE WITH TABLE ... PROPERTIES('swap'='true')。
--   E3    ✅ 不带 PROPERTIES 时 swap 默认为 true，**两张表都还在**（不删表）；
--            但生成 SQL 一律显式带 swap=true，不依赖默认值。
--   E4    ✅ 交换后**数据与 schema 都跟着名字走**：原 a2 的对象（含其列定义与
--            数据）出现在名字 a1 下，反之亦然；两张表都保留。
--   E5    ✅ 该语句能经 FE HTTP 通道执行（`POST /api/query/default_cluster/{db}`
--            + `{"stmt":...}`），与 DorisMetadataApplier 的实际通道一致。
--   E6    ⚠️ 第二张表名**不能带库名**：`REPLACE WITH TABLE db2.t2` 是语法错误，
--            表名只在第一张表的库里解析 → **跨库 rename 无法用 REPLACE 表达**
--            （`REPLACE WITH TABLE c2` 报 unknown table）→ 上游应 fail-fast。
--   EA    ❌ `ALTER TABLE g1 RENAME g2`（g2 已存在）报
--            `Table name[g2] is already used` → 冲突必须先于 ALTER RENAME 处理。
--   EB    ❌ `ALTER TABLE g1 RENAME g1`（自改名，旧 bug 会生成这条）报
--            `Same table name` → 旧实现会让作业在 Doris 上直接失败。
--   EC    ❌ 一条 `ALTER TABLE` 只能改名一张表（`RENAME g2, g3` 语法错误）
--            → Doris 侧多条 DDL 只能拆成多条语句。
-- ============================================================================

CREATE DATABASE IF NOT EXISTS rename_swap_exp;
USE rename_swap_exp;

-- ---------------------------------------------------------------------------
-- E1 + E2：结构不同的两表互换（UNIQUE 有主键 ↔ DUPLICATE 无主键）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS a1;
DROP TABLE IF EXISTS a2;
CREATE TABLE a1 (id INT, v1 VARCHAR(20))
    UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
CREATE TABLE a2 (k INT, v2 VARCHAR(50), extra BIGINT)
    DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
INSERT INTO a1 VALUES (1, 'a1row');
INSERT INTO a2 VALUES (2, 'a2row', 99);

-- 交换前：a1 是 UNIQUE(id, v1)，a2 是 DUPLICATE(k, v2, extra)
SHOW TABLES;
SHOW CREATE TABLE a1;
SHOW CREATE TABLE a2;
SELECT * FROM a1; -- 1, a1row
SELECT * FROM a2; -- 2, a2row, 99

ALTER TABLE a1 REPLACE WITH TABLE a2 PROPERTIES('swap' = 'true');

-- E4 交换后：a1 变成 DUPLICATE(k, v2, extra) 且数据是 2, a2row, 99；
--          a2 变成 UNIQUE(id, v1) 且数据是 1, a1row；两张表都还在。
SHOW TABLES;
SHOW CREATE TABLE a1;
SHOW CREATE TABLE a2;
SELECT * FROM a1;
SELECT * FROM a2;

-- ---------------------------------------------------------------------------
-- E3：不带 PROPERTIES（确认默认 swap=true 且不删表）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS b1;
DROP TABLE IF EXISTS b2;
CREATE TABLE b1 (id INT, v1 VARCHAR(20))
    UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
CREATE TABLE b2 (id INT, v1 VARCHAR(20))
    UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
INSERT INTO b1 VALUES (1, 'b1row');
INSERT INTO b2 VALUES (2, 'b2row');

ALTER TABLE b1 REPLACE WITH TABLE b2; -- 实测：等效 swap=true，b2 未被 DROP
SELECT * FROM b1;                     -- 2, b2row
SELECT COUNT(*) FROM b2;              -- 表仍在

-- ---------------------------------------------------------------------------
-- E6：语法约束 —— 第二张表名不能限定库名；跨库解析不到
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS c1;
DROP TABLE IF EXISTS d1;
CREATE TABLE c1 (id INT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
CREATE TABLE d1 (id INT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');

-- ✅ 正确形式：第一张可限定库名，第二张必须是裸表名
ALTER TABLE rename_swap_exp.c1 REPLACE WITH TABLE d1 PROPERTIES('swap' = 'true');

-- ❌ 错误形式（语法错误，报在第二个 `.` 上）：
-- ALTER TABLE rename_swap_exp.c1 REPLACE WITH TABLE rename_swap_exp.d1 PROPERTIES('swap'='true');
-- ❌ 跨库（第二张在别的库，报 unknown table, tableName=c2）：
-- ALTER TABLE rename_swap_exp.c1 REPLACE WITH TABLE c2 PROPERTIES('swap'='true');

-- ---------------------------------------------------------------------------
-- EA / EB / EC：为什么必须先做冲突检测（旧实现会生成 EB 那条）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS g1;
DROP TABLE IF EXISTS g2;
CREATE TABLE g1 (id INT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');
CREATE TABLE g2 (id INT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO
    PROPERTIES('replication_allocation' = 'tag.location.default: 1');

-- EA ❌ Table name[g2] is already used
-- ALTER TABLE g1 RENAME g2;
-- EB ❌ Same table name
-- ALTER TABLE g1 RENAME g1;
-- EC ❌ 语法错误（一条 ALTER 只能改名一张表）
-- ALTER TABLE g1 RENAME g2, g3;

-- ---------------------------------------------------------------------------
-- E5：HTTP 通道（DorisMetadataApplier 的真实执行路径）
-- ---------------------------------------------------------------------------
-- 前置：Doris 的 HTTP 鉴权要求用户**有密码**（空密码用户被拒：
--       Access denied for user 'root' ... (using password: YES)），
--       实验用临时用户 cdc_probe（跑完已 DROP）：
--         CREATE USER 'cdc_probe'@'%' IDENTIFIED BY '<password>';
--         GRANT ALL ON rename_swap_exp.* TO 'cdc_probe'@'%';
--
-- curl -u cdc_probe:<password> -X POST \
--   "http://<fe>:8030/api/query/default_cluster/rename_swap_exp" \
--   -H 'Content-Type: application/json' -d '{"stmt":"SELECT 1"}'
--     → result_set（通道可用）
-- curl ... -d '{"stmt":"ALTER TABLE `rename_swap_exp`.`e2` RENAME `e2_renamed`"}'
--     → {"msg":"success","code":0}
-- curl ... -d '{"stmt":"ALTER TABLE `rename_swap_exp`.`e2_renamed` REPLACE WITH TABLE `f2` PROPERTIES(\"swap\" = \"true\")"}'
--     → {"msg":"success","code":0}（REPLACE 也能走 HTTP）
