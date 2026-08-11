/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.canal.source.ddl;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChangeType;

import javax.annotation.Nullable;

/**
 * The result of parsing one canal DDL message: the affected table, the type of the schema change
 * and the resulting table schema.
 */
public class CanalDdlParsedResult {

    private final TableId tableId;
    private final TableChangeType type;
    @Nullable private final Table table;

    public CanalDdlParsedResult(TableId tableId, TableChangeType type, @Nullable Table table) {
        this.tableId = tableId;
        this.type = type;
        this.table = table;
    }

    public TableId getTableId() {
        return tableId;
    }

    public TableChangeType getType() {
        return type;
    }

    /**
     * Returns the resulting table schema for a {@code CREATE}/{@code ALTER} change, or {@code null}
     * for a {@code DROP}.
     */
    @Nullable
    public Table getTable() {
        return table;
    }
}
