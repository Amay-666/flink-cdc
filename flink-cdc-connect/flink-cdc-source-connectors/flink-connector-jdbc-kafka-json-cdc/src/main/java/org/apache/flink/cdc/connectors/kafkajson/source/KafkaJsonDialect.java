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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.connection.KafkaJsonConnectionPoolFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.connection.KafkaJsonJdbcConnection;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonScanFetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonStreamFetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.kafka.KafkaJsonKafkaOffsetUtils;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The MySQL dialect for the Canal source — the default selection of {@code scan.database.type}.
 *
 * <p>The snapshot data is read directly from MySQL through JDBC (using the incremental snapshot
 * algorithm of flink-cdc-base), while the change-log data is consumed from Kafka where canal has
 * written the binlog change events. The {@code displayCurrentOffset} therefore queries the current
 * Kafka position instead of the binlog position.
 *
 * <p>TiDB speaks the same wire protocol and shares this implementation; its TiDB-specific behavior
 * (the TSO snapshot boundary) lives in {@link KafkaJsonTiDBDialect}, selected by {@code
 * scan.database.type=tidb} through {@link KafkaJsonDialectFactory}.
 */
public class KafkaJsonDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;

    private final KafkaJsonSourceConfig sourceConfig;

    // The dialect is a single instance shared by every subtask (and the split enumerator), so these
    // lazily-built fields may be initialized and used from several threads (e.g. the enumerator's
    // chunk splitter and the stream-split reader both call queryTableSchema). They are volatile and
    // built under a double-checked lock; the cached KafkaJsonSchema itself serializes its mutations
    // (see KafkaJsonSchema#getTableSchema).
    private transient volatile KafkaJsonSchema schema;
    private transient volatile Tables.TableFilter filters;
    @Nullable private transient Supplier<KafkaJsonOffset> currentOffsetSupplier;
    @Nullable private KafkaJsonStreamFetchTask streamFetchTask;

    public KafkaJsonDialect(KafkaJsonSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        KafkaJsonSourceConfig canalSourceConfig = (KafkaJsonSourceConfig) sourceConfig;
        MySqlConnectorConfig dbzConfig = canalSourceConfig.getDbzConnectorConfig();
        // MySQL identifiers are quoted with backticks. The KafkaJsonJdbcConnection drops the column
        // default value that the MySQL driver reports as a literal (e.g. `0x` for a BINARY
        // default);
        // feeding it into Debezium's TableSchemaBuilder fails the snapshot schema read.
        return new KafkaJsonJdbcConnection(
                dbzConfig.getJdbcConfig(),
                new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                "`",
                "`");
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        if (currentOffsetSupplier != null) {
            return currentOffsetSupplier.get();
        }
        return KafkaJsonKafkaOffsetUtils.queryCurrentOffset((KafkaJsonSourceConfig) sourceConfig);
    }

    /** Injects a supplier of the current stream offset (used in unit tests). */
    @VisibleForTesting
    public void setCurrentOffsetSupplierForTesting(Supplier<KafkaJsonOffset> supplier) {
        this.currentOffsetSupplier = supplier;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // MySQL is case-sensitive about table names on Linux
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new KafkaJsonChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return KafkaJsonTableDiscoveryUtils.listTables(
                    sourceConfig.getDatabaseList().get(0), jdbc, sourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                tableSchemas.put(tableId, queryTableSchema(jdbc, tableId));
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new KafkaJsonConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        KafkaJsonSchema localSchema = schema;
        if (localSchema == null) {
            synchronized (this) {
                localSchema = schema;
                if (localSchema == null) {
                    localSchema = new KafkaJsonSchema(sourceConfig);
                    schema = localSchema;
                }
            }
        }
        // getTableSchema is synchronized on the schema, so concurrent chunk-splitting (enumerator
        // thread) and stream-split schema discovery (reader thread) never corrupt the cached map
        return localSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new KafkaJsonScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new KafkaJsonStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new KafkaJsonSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        Tables.TableFilter filter = filters;
        if (filter == null) {
            synchronized (this) {
                filter = filters;
                if (filter == null) {
                    filter = sourceConfig.getTableFilters().dataCollectionFilter();
                    filters = filter;
                }
            }
        }
        return filter.isIncluded(tableId);
    }
}
