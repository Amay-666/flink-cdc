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

package org.apache.flink.cdc.connectors.canal.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.canal.source.CanalSource;
import org.apache.flink.cdc.connectors.canal.source.CanalSourceBuilder;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Canal source (MySQL snapshot via JDBC
 * + incremental changes and DDL consumed from Kafka) from a logical description.
 */
public class CanalTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String tableName;
    private final String username;
    private final String password;
    private final ZoneId serverTimeZone;
    private final Properties dbzProperties;
    private final StartupOptions startupOptions;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final int connectMaxRetries;
    private final int connectionPoolSize;
    @Nullable private final String chunkKeyColumn;
    private final boolean skipSnapshotBackfill;
    private final boolean closeIdleReaders;
    private final boolean scanNewlyAddedTableEnabled;
    private final String kafkaBootstrapServers;
    @Nullable private final String kafkaGroupId;
    private final List<String> kafkaTopics;
    private final CanalSourceOptions.MessageFormat messageFormat;
    private final CanalSourceOptions.EventTime eventTime;
    private final CanalSourceOptions.BoundaryMode boundaryMode;
    private final CanalSourceOptions.KafkaStartupMode kafkaStartupMode;
    private final CanalSourceOptions.DdlParser ddlParser;
    private final Properties kafkaProperties;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public CanalTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean closeIdleReaders,
            boolean scanNewlyAddedTableEnabled,
            String kafkaBootstrapServers,
            @Nullable String kafkaGroupId,
            List<String> kafkaTopics,
            CanalSourceOptions.MessageFormat messageFormat,
            CanalSourceOptions.EventTime eventTime,
            CanalSourceOptions.BoundaryMode boundaryMode,
            CanalSourceOptions.KafkaStartupMode kafkaStartupMode,
            CanalSourceOptions.DdlParser ddlParser,
            Properties kafkaProperties) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.startupOptions = checkNotNull(startupOptions);
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.chunkKeyColumn = chunkKeyColumn;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.closeIdleReaders = closeIdleReaders;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.kafkaBootstrapServers = checkNotNull(kafkaBootstrapServers);
        this.kafkaGroupId = kafkaGroupId;
        this.kafkaTopics = checkNotNull(kafkaTopics);
        this.messageFormat = messageFormat;
        this.eventTime = eventTime;
        this.boundaryMode = boundaryMode;
        this.kafkaStartupMode = kafkaStartupMode;
        this.ddlParser = ddlParser;
        this.kafkaProperties = kafkaProperties;
        // Mutable attributes
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    /** Returns the startup options of this source. */
    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = getMetadataConverters();
        final TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(serverTimeZone)
                        .setUserDefinedConverterFactory(
                                CanalDeserializationConverterFactory.instance())
                        .build();

        CanalSource<RowData> source =
                CanalSourceBuilder.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .databaseList(database)
                        // MySQL identifiers are quoted in the include-list pattern with a literal
                        // dot; the discovery uses the database-name/table-name separately
                        .tableList(tableName)
                        .username(username)
                        .password(password)
                        .serverTimeZone(serverTimeZone.toString())
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .startupOptions(startupOptions)
                        .chunkKeyColumn(chunkKeyColumn)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                        .closeIdleReaders(closeIdleReaders)
                        .kafkaBootstrapServers(kafkaBootstrapServers)
                        .kafkaGroupId(kafkaGroupId)
                        .kafkaTopics(kafkaTopics.toArray(new String[0]))
                        .messageFormat(messageFormat)
                        .eventTime(eventTime)
                        .boundaryMode(boundaryMode)
                        .kafkaStartupMode(kafkaStartupMode)
                        .ddlParser(ddlParser)
                        .kafkaProperties(kafkaProperties)
                        .debeziumProperties(dbzProperties)
                        .deserializer(deserializer)
                        .build();
        return SourceProvider.of(source);
    }

    protected MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(CanalReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(CanalReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        // Return metadata in a fixed order
        return Stream.of(CanalReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                CanalReadableMetadata::getKey,
                                CanalReadableMetadata::getDataType,
                                (existingValue, newValue) -> newValue,
                                LinkedHashMap::new));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        CanalTableSource source =
                new CanalTableSource(
                        physicalSchema,
                        port,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        serverTimeZone,
                        dbzProperties,
                        startupOptions,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetries,
                        connectionPoolSize,
                        chunkKeyColumn,
                        skipSnapshotBackfill,
                        closeIdleReaders,
                        scanNewlyAddedTableEnabled,
                        kafkaBootstrapServers,
                        kafkaGroupId,
                        kafkaTopics,
                        messageFormat,
                        eventTime,
                        boundaryMode,
                        kafkaStartupMode,
                        ddlParser,
                        kafkaProperties);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CanalTableSource)) {
            return false;
        }
        CanalTableSource that = (CanalTableSource) o;
        return port == that.port
                && splitSize == that.splitSize
                && splitMetaGroupSize == that.splitMetaGroupSize
                && fetchSize == that.fetchSize
                && connectMaxRetries == that.connectMaxRetries
                && connectionPoolSize == that.connectionPoolSize
                && skipSnapshotBackfill == that.skipSnapshotBackfill
                && closeIdleReaders == that.closeIdleReaders
                && scanNewlyAddedTableEnabled == that.scanNewlyAddedTableEnabled
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(kafkaBootstrapServers, that.kafkaBootstrapServers)
                && Objects.equals(kafkaGroupId, that.kafkaGroupId)
                && Objects.equals(kafkaTopics, that.kafkaTopics)
                && messageFormat == that.messageFormat
                && eventTime == that.eventTime
                && boundaryMode == that.boundaryMode
                && kafkaStartupMode == that.kafkaStartupMode
                && ddlParser == that.ddlParser
                && Objects.equals(kafkaProperties, that.kafkaProperties)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                tableName,
                username,
                password,
                serverTimeZone,
                dbzProperties,
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                closeIdleReaders,
                scanNewlyAddedTableEnabled,
                kafkaBootstrapServers,
                kafkaGroupId,
                kafkaTopics,
                messageFormat,
                eventTime,
                boundaryMode,
                kafkaStartupMode,
                ddlParser,
                kafkaProperties,
                producedDataType,
                metadataKeys);
    }

    @Override
    public String asSummaryString() {
        return "Canal-CDC";
    }
}
