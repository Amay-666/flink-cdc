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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordFactory;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The database schema of the Canal source.
 *
 * <p>It is a {@link RelationalDatabaseSchema} so that the flink-cdc-base incremental-snapshot
 * machinery ({@code isRecordBetween}, the event dispatcher, the deserialization) can look up the
 * {@link Table} and {@link TableSchema} of a record. All schema state is owned by a single shared
 * {@link KafkaJsonRecordFactory} so that the snapshot rows and the streaming (canal) messages build
 * byte-identical {@code SourceRecord}s.
 *
 * <p>The table metadata is sourced from the canal flatMessage {@code mysqlType} (streaming) or from
 * MySQL via JDBC (snapshot); this class caches the JDBC-derived {@link TableChange} used by the
 * snapshot splitter.
 */
public class KafkaJsonSchema extends io.debezium.relational.RelationalDatabaseSchema {

    // cache the schema for each table (used by the snapshot splitter)
    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>();
    private final KafkaJsonSourceConfig sourceConfig;
    private final KafkaJsonRecordFactory recordFactory;

    /** Creates a standalone schema (used by the snapshot splitter / dialect). */
    public KafkaJsonSchema(KafkaJsonSourceConfig sourceConfig) {
        this(sourceConfig, new KafkaJsonRecordFactory(sourceConfig));
    }

    /** Creates a schema that shares the given record factory (used by the fetch task context). */
    public KafkaJsonSchema(
            KafkaJsonSourceConfig sourceConfig, KafkaJsonRecordFactory recordFactory) {
        super(
                sourceConfig.getDbzConnectorConfig(),
                MySqlTopicSelector.defaultSelector(sourceConfig.getDbzConnectorConfig()),
                sourceConfig.getTableFilters().dataCollectionFilter(),
                sourceConfig.getDbzConnectorConfig().getColumnFilter(),
                recordFactory.getSchemaBuilder(),
                false,
                null);
        this.sourceConfig = sourceConfig;
        this.recordFactory = recordFactory;
    }

    /**
     * Returns the {@link TableChange} of the given table, reading it from MySQL if not cached.
     *
     * <p>This is the only mutation entry point of the shared schema held by {@code
     * KafkaJsonDialect} (a single instance used by every subtask thread): the split enumerator's
     * chunk splitter and the stream-split reader both resolve table schemas here. The method is
     * synchronized so concurrent calls never corrupt the {@link #schemasByTableId} cache.
     * Per-subtask schemas (owned by {@code KafkaJsonSourceFetchTaskContext}) are single-threaded
     * and unaffected.
     */
    public synchronized TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                readTableSchema(jdbc, tableId);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    /** Registers (or replaces) the given table in the underlying record factory. */
    public synchronized void registerTable(Table table) {
        recordFactory.registerTable(table);
    }

    /** Removes the given table (e.g. on a {@code DROP TABLE} DDL). */
    public synchronized void removeTable(TableId tableId) {
        schemasByTableId.remove(tableId);
        recordFactory.removeTable(tableId);
    }

    @Override
    public Table tableFor(TableId id) {
        return recordFactory.tableFor(id);
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        return recordFactory.tableSchemaFor(id);
    }

    @Override
    public Set<TableId> tableIds() {
        return recordFactory.tableIds();
    }

    /** Returns the {@link MySqlConnectorConfig} used by this schema. */
    public MySqlConnectorConfig getConnectorConfig() {
        return sourceConfig.getDbzConnectorConfig();
    }

    private void readTableSchema(JdbcConnection jdbc, TableId tableId) throws SQLException {
        Tables tables = new Tables();
        jdbc.readSchema(
                tables,
                tableId.catalog(),
                null,
                sourceConfig.getTableFilters().dataCollectionFilter(),
                null,
                false);

        Table table = Objects.requireNonNull(tables.forTable(tableId));
        TableChange tableChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        this.schemasByTableId.put(tableId, tableChange);
        // register in the record factory so the fetch task can build records for this table
        recordFactory.registerTable(table);
    }
}
