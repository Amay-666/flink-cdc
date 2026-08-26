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

package org.apache.flink.cdc.connectors.kafkajson.source.fetch;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonSchema;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.kafka.KafkaJsonOffsetSupplier;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordConverter;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetContext;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonPartition;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonChunkUtils;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonKafkaUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.TopicSelector;
import io.debezium.util.LoggingContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;

/**
 * The fetch task context of the Canal source, shared by {@link KafkaJsonScanFetchTask} and {@link
 * KafkaJsonStreamFetchTask}.
 *
 * <p>It owns the {@link ChangeEventQueue} that both fetch tasks enqueue {@link DataChangeEvent}s
 * into, the {@link JdbcSourceEventDispatcher} used to dispatch the low/high/end watermark events of
 * the incremental-snapshot algorithm, and the shared {@link KafkaJsonSchema}/{@link
 * KafkaJsonRecordFactory} that keep the table schemas. The streaming reader additionally owns a
 * {@link KafkaConsumer} and a {@link KafkaJsonOffsetSupplier}.
 */
public class KafkaJsonSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaJsonSourceFetchTaskContext.class);

    private final KafkaJsonSourceConfig canalSourceConfig;

    // stateful objects built once per reader lifetime
    private KafkaJsonRecordFactory recordFactory;
    private KafkaJsonRecordConverter recordConverter;
    private KafkaJsonSchema schema;
    @Nullable private KafkaConsumer<String, String> kafkaConsumer;
    @Nullable private KafkaJsonOffsetSupplier offsetSupplier;
    @Nullable private JdbcConnection jdbcConnection;

    // per-split objects rebuilt on every configure()
    private ChangeEventQueue<DataChangeEvent> queue;
    private JdbcSourceEventDispatcher<KafkaJsonPartition> dispatcher;
    private KafkaJsonOffsetContext offsetContext;
    private KafkaJsonPartition partition;
    private ErrorHandler errorHandler;

    public KafkaJsonSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, KafkaJsonDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.canalSourceConfig = (KafkaJsonSourceConfig) sourceConfig;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        LOG.debug("Configuring KafkaJsonSourceFetchTaskContext for split: {}", sourceSplitBase);
        MySqlConnectorConfig connectorConfig = getDbzConnectorConfig();

        if (schema == null) {
            this.recordFactory = new KafkaJsonRecordFactory(canalSourceConfig);
            this.recordConverter = new KafkaJsonRecordConverter(recordFactory, canalSourceConfig);
            this.schema = new KafkaJsonSchema(canalSourceConfig, recordFactory);
        }
        // register the table schemas carried by the split so the snapshot read can resolve them.
        // The splitter builds its schemas with a separate KafkaJsonRecordFactory instance, so the
        // shared schema must be (re)populated here.
        for (TableChange tableChange : sourceSplitBase.getTableSchemas().values()) {
            schema.registerTable(tableChange.getTable());
        }

        this.offsetContext = new KafkaJsonOffsetContext(connectorConfig);
        this.partition = new KafkaJsonPartition(connectorConfig.getLogicalName());

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(connectorConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "canal",
                                                connectorConfig.getLogicalName(),
                                                "jdbc-kafka-json-cdc-connector-task"))
                        // do not buffer any element, we use signal events
                        .build();

        this.errorHandler = new ErrorHandler(MySqlConnector.class, connectorConfig, queue);
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        schema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        new KafkaJsonEventMetadataProvider(),
                        schemaNameAdjuster,
                        new KafkaJsonSchemaChangeEventHandler());
    }

    @Override
    public KafkaJsonSourceConfig getSourceConfig() {
        return canalSourceConfig;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        // The base implementation (SourceRecordUtils.getTableId) also reads
        // source.getString("schema"), which our source struct intentionally does not carry: MySQL
        // (and TiDB over the MySQL protocol) has no separate schema namespace, so
        // KafkaJsonSourceInfoStructMaker only emits db + table. Parse the two fields that exist.
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
        String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
        return new TableId(dbName, null, tableName);
    }

    /** Returns the {@link KafkaJsonRecordConverter} that turns canal flatMessages into records. */
    public KafkaJsonRecordConverter getRecordConverter() {
        return recordConverter;
    }

    /**
     * Returns (creating on first use) the Kafka consumer used by the streaming reader.
     *
     * <p>The consumer is created lazily so that snapshot-only readers never open a Kafka
     * connection.
     */
    public KafkaConsumer<String, String> getKafkaConsumer() {
        if (kafkaConsumer == null) {
            kafkaConsumer =
                    new KafkaConsumer<>(
                            KafkaJsonKafkaUtils.buildConsumerProps(
                                    canalSourceConfig, canalSourceConfig.getKafkaGroupId()));
        }
        return kafkaConsumer;
    }

    /** Returns (creating on first use) the supplier of the current stream position. */
    public KafkaJsonOffsetSupplier getOffsetSupplier() {
        if (offsetSupplier == null) {
            offsetSupplier = new KafkaJsonOffsetSupplier(canalSourceConfig);
        }
        return offsetSupplier;
    }

    /** Returns the {@link KafkaJsonRecordFactory} shared by the schema and the record converter. */
    public KafkaJsonRecordFactory getRecordFactory() {
        return recordFactory;
    }

    /**
     * Returns (creating on first use) the JDBC connection used to read the snapshot data.
     *
     * <p>The connection is created lazily so that stream-only readers never open a MySQL
     * connection.
     */
    public JdbcConnection getConnection() {
        if (jdbcConnection == null) {
            jdbcConnection = dataSourceDialect.openJdbcConnection(canalSourceConfig);
        }
        return jdbcConnection;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return KafkaJsonOffset.of(sourceRecord);
    }

    @Override
    public KafkaJsonSchema getDatabaseSchema() {
        return schema;
    }

    @Override
    public RowType getSplitType(Table table) {
        Column splitColumn =
                KafkaJsonChunkUtils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
        return KafkaJsonChunkUtils.getSplitType(splitColumn);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public JdbcSourceEventDispatcher<KafkaJsonPartition> getDispatcher() {
        return dispatcher;
    }

    @Override
    public KafkaJsonOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public KafkaJsonPartition getPartition() {
        return partition;
    }

    @Override
    public MySqlConnectorConfig getDbzConnectorConfig() {
        return (MySqlConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public void close() throws Exception {
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
            } catch (Exception e) {
                LOG.warn("Failed to close the KafkaJsonSourceFetchTaskContext kafka consumer", e);
            }
            kafkaConsumer = null;
        }
        if (offsetSupplier != null) {
            offsetSupplier.close();
            offsetSupplier = null;
        }
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                LOG.warn("Failed to close the KafkaJsonSourceFetchTaskContext jdbc connection", e);
            }
            jdbcConnection = null;
        }
    }

    /** Injects the Kafka consumer used by the streaming reader (used in unit tests). */
    @VisibleForTesting
    public void setKafkaConsumerForTesting(KafkaConsumer<String, String> consumer) {
        this.kafkaConsumer = consumer;
    }

    /** Injects the JDBC connection used to read the snapshot data (used in unit tests). */
    @VisibleForTesting
    public void setJdbcConnectionForTesting(JdbcConnection connection) {
        this.jdbcConnection = connection;
    }
}
