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

package org.apache.flink.cdc.connectors.canal.source;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.connection.CanalConnectionPoolFactory;
import org.apache.flink.cdc.connectors.canal.source.fetch.CanalScanFetchTask;
import org.apache.flink.cdc.connectors.canal.source.fetch.CanalSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.canal.source.fetch.CanalStreamFetchTask;
import org.apache.flink.cdc.connectors.canal.source.kafka.CanalKafkaOffsetUtils;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffset;
import org.apache.flink.cdc.connectors.canal.source.utils.CanalTableDiscoveryUtils;
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
 * The dialect for the Canal source.
 *
 * <p>The snapshot data is read directly from MySQL through JDBC (using the incremental snapshot
 * algorithm of flink-cdc-base), while the change-log data is consumed from Kafka where canal has
 * written the binlog change events. The {@code displayCurrentOffset} therefore queries the current
 * Kafka position instead of the binlog position.
 */
public class CanalDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;

    private final CanalSourceConfig sourceConfig;
    private transient CanalSchema schema;
    private transient Tables.TableFilter filters;
    @Nullable private transient Supplier<CanalOffset> currentOffsetSupplier;
    @Nullable private CanalStreamFetchTask streamFetchTask;

    public CanalDialect(CanalSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        CanalSourceConfig canalSourceConfig = (CanalSourceConfig) sourceConfig;
        MySqlConnectorConfig dbzConfig = canalSourceConfig.getDbzConnectorConfig();
        // MySQL identifiers are quoted with backticks
        return new JdbcConnection(
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
        return CanalKafkaOffsetUtils.queryCurrentOffset((CanalSourceConfig) sourceConfig);
    }

    /** Injects a supplier of the current stream offset (used in unit tests). */
    @VisibleForTesting
    public void setCurrentOffsetSupplierForTesting(Supplier<CanalOffset> supplier) {
        this.currentOffsetSupplier = supplier;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // MySQL is case-sensitive about table names on Linux
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new CanalChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return CanalTableDiscoveryUtils.listTables(
                    sourceConfig.getDatabaseList().get(0),
                    jdbc,
                    sourceConfig.getTableFilters());
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
        return new CanalConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new CanalSchema(sourceConfig);
        }
        return schema.getTableSchema(jdbc, tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new CanalScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new CanalStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new CanalSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }
        return filters.isIncluded(tableId);
    }
}
