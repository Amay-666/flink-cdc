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

package org.apache.flink.cdc.connectors.kafkajson.source.message;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.convert.KafkaJsonValueConverter;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonPartition;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfoStructMaker;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the Debezium-shaped {@link SourceRecord}s for the Canal source.
 *
 * <p>Both the snapshot phase (rows read from MySQL via JDBC) and the streaming phase (canal
 * flatMessages consumed from Kafka) produce their records through this single factory so that the
 * {@code after}/{@code before}/{@code key} structs, the {@code source} struct and the envelope stay
 * byte-identical across the two phases.
 *
 * <p>The table schema (columns → Kafka Connect structs, using the {@link MySqlValueConverters})
 * is registered per {@link TableId} and cached; it is refreshed on DDL events.
 */
public class KafkaJsonRecordFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<TableId, TableSchema> tableSchemas = new HashMap<>();
    private final Map<TableId, Table> tables = new HashMap<>();
    private final TableSchemaBuilder schemaBuilder;
    private final KafkaJsonSourceInfoStructMaker sourceInfoStructMaker;
    private final Schema sourceInfoSchema;
    private final KafkaJsonValueConverter valueConverter;
    private final String serverName;

    public KafkaJsonRecordFactory(KafkaJsonSourceConfig sourceConfig) {
        MySqlConnectorConfig dbzConfig = sourceConfig.getDbzConnectorConfig();
        this.sourceInfoStructMaker =
                new KafkaJsonSourceInfoStructMaker(
                        "canal", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION, dbzConfig);
        this.sourceInfoSchema = sourceInfoStructMaker.schema();
        MySqlValueConverters valueConverters = createValueConverters(dbzConfig);
        this.schemaBuilder =
                new TableSchemaBuilder(
                        valueConverters,
                        null,
                        SchemaNameAdjuster.create(),
                        new CustomConverterRegistry(Collections.emptyList()),
                        sourceInfoSchema,
                        false,
                        false);
        this.valueConverter = new KafkaJsonValueConverter(ZoneId.of(sourceConfig.getServerTimeZone()));
        this.serverName = dbzConfig.getLogicalName();
    }

    /** The logical server name ({@code source.name}) used for the record partition. */
    public String getServerName() {
        return serverName;
    }

    /** The underlying {@link TableSchemaBuilder}, shared with {@code KafkaJsonSchema}. */
    public TableSchemaBuilder getSchemaBuilder() {
        return schemaBuilder;
    }

    /** The set of tables whose schema has been registered so far. */
    public Set<TableId> tableIds() {
        return tables.keySet();
    }

    /** Registers (or replaces) the schema of the given table. */
    public void registerTable(Table table) {
        TableId tableId = table.id();
        tables.put(tableId, table);
        tableSchemas.put(tableId, createTableSchema(table));
    }

    /** Removes the schema of the given table (e.g. on a {@code DROP TABLE} DDL). */
    public void removeTable(TableId tableId) {
        tables.remove(tableId);
        tableSchemas.remove(tableId);
    }

    private TableSchema createTableSchema(Table table) {
        String envelopSchemaName =
                "io.debezium.connector.mysql." + serverName + ".Envelope";
        return schemaBuilder.create("", envelopSchemaName, table, null, null, null);
    }

    @Nullable
    public TableSchema tableSchemaFor(TableId tableId) {
        return tableSchemas.get(tableId);
    }

    @Nullable
    public Table tableFor(TableId tableId) {
        return tables.get(tableId);
    }

    public boolean hasTable(TableId tableId) {
        return tableSchemas.containsKey(tableId);
    }

    /**
     * Converts a canal row ({@code column name -> String value}) into the typed column-data array
     * expected by {@link TableSchema#valueFromColumnData(Object[])}. Values are converted according
     * to the column type via {@link KafkaJsonValueConverter}.
     */
    public Object[] canalRowData(Table table, Map<String, String> row) {
        List<Column> columns = table.columns();
        Object[] data = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            data[i] = valueConverter.convert(column, row.get(column.name()));
        }
        return data;
    }

    /**
     * Builds a Debezium-shaped {@link SourceRecord} for one row.
     *
     * @param table the table the row belongs to (its schema must be registered)
     * @param beforeData the {@code before} column data, or {@code null} for INSERT/READ
     * @param afterData the {@code after} column data, or {@code null} for DELETE
     * @param op the envelope operation ({@code c}/{@code r}/{@code u}/{@code d})
     * @param sourceInfo the source info of the record
     * @param topic the Kafka topic the message was consumed from
     * @param kafkaPartition the Kafka partition
     * @param kafkaOffset the Kafka partition-local offset
     */
    public SourceRecord createRecord(
            Table table,
            @Nullable Object[] beforeData,
            @Nullable Object[] afterData,
            Envelope.Operation op,
            KafkaJsonSourceInfo sourceInfo,
            String topic,
            int kafkaPartition,
            long kafkaOffset) {
        TableSchema tableSchema = tableSchemas.get(table.id());
        if (tableSchema == null) {
            throw new IllegalStateException(
                    "No table schema registered for " + table.id() + "; register the table first");
        }
        // For DELETE the key must be built from the before image, otherwise from the after image;
        // tables without a primary key have no key schema/generator
        Object[] keyData = afterData != null ? afterData : beforeData;
        Struct key =
                tableSchema.keySchema() == null
                        ? null
                        : tableSchema.keyFromColumnData(keyData);
        Struct source = sourceInfoStructMaker.struct(sourceInfo);

        Envelope envelope = tableSchema.getEnvelopeSchema();
        Struct before = beforeData == null ? null : tableSchema.valueFromColumnData(beforeData);
        Struct after = afterData == null ? null : tableSchema.valueFromColumnData(afterData);
        Instant timestamp = Instant.ofEpochMilli(sourceInfo.getEventTime());
        Struct value;
        switch (op) {
            case READ:
                value = envelope.read(after, source, timestamp);
                break;
            case CREATE:
                value = envelope.create(after, source, timestamp);
                break;
            case UPDATE:
                value = envelope.update(before, after, source, timestamp);
                break;
            case DELETE:
                value = envelope.delete(before, source, timestamp);
                break;
            default:
                throw new IllegalStateException("Unsupported envelope operation: " + op);
        }

        Map<String, ?> sourcePartition = new KafkaJsonPartition(serverName).getSourcePartition();
        Map<String, ?> sourceOffset =
                new KafkaJsonOffset(sourceInfo.getEventTime(), kafkaPartition, kafkaOffset).getOffset();
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                kafkaPartition,
                tableSchema.keySchema(),
                key,
                envelope.schema(),
                value,
                sourceInfo.getEventTime(),
                null);
    }

    private static MySqlValueConverters createValueConverters(MySqlConnectorConfig dbzConfig) {
        TemporalPrecisionMode timePrecisionMode = dbzConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = dbzConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                dbzConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
                dbzConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                dbzConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }
}
