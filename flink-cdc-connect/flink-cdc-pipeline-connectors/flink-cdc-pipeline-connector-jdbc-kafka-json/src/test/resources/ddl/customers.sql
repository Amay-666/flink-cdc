-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  customers
-- ----------------------------------------------------------------------------------------------------------------
-- A deliberately simple table (an integer primary key and two varchar columns) shared by the MySQL and
-- the TiDB integration tests. TiDB is reached through the MySQL-compatible wire protocol, so the DDL stays
-- portable and avoids TiDB-specific column types (AUTO_RANDOM, vector, ...).
CREATE TABLE customers (
    id      INT         NOT NULL,
    name    VARCHAR(255) NOT NULL,
    address VARCHAR(255),
    PRIMARY KEY (id)
);

INSERT INTO customers (id, name, address)
VALUES (101, 'user_1', 'Shanghai'),
       (102, 'user_2', 'Beijing'),
       (103, 'user_3', 'Hangzhou'),
       (104, 'user_4', 'Shenzhen');
