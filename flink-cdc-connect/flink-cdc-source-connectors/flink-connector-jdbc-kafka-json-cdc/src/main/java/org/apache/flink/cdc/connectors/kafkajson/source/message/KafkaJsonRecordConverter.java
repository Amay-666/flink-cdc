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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.util.FlinkRuntimeException;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts a change-log message into the list of Debezium-shaped {@link SourceRecord}s, dispatching
 * on the message format (see docs/DEBEZIUM_PLAN.md §S3).
 *
 * <p><b>canal flatMessage</b> — batches the rows of a single DML statement into {@code data[]}/
 * {@code old[]}; each row becomes one {@link SourceRecord}:
 *
 * <ul>
 *   <li>{@code INSERT} → {@code op=c} with {@code after}=data row;
 *   <li>{@code UPDATE} → {@code op=u} with {@code before}=old row and {@code after}=data row;
 *   <li>{@code DELETE} → {@code op=d} with {@code before}=data row;
 *   <li>other types ({@code GTID}/{@code SAVEPOINT}/…) and DDL messages produce no data records.
 * </ul>
 *
 * <p><b>Debezium envelope</b> — a single typed {@code before}/{@code after} image per message, with
 * the {@code op} already in Debezium's {@code c}/{@code u}/{@code d}/{@code r} coding; the table
 * schema comes from the registered (JDBC or DDL) schema.
 *
 * <p>A message's own type metadata is never used to build the table schema (see {@link
 * TableSchemaResolver}); a table whose first data change arrives before any schema was registered
 * is read through JDBC, and a failure to do so fails the job instead of dropping the rows.
 *
 * <p>DDL messages are forwarded to the schema-change pipeline (Phase 8) and therefore skipped here.
 */
public class KafkaJsonRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonRecordConverter.class);

    private final KafkaJsonRecordFactory factory;
    private final KafkaJsonSourceConfig sourceConfig;
    private final EventTime eventTimeMode;
    private final MySqlConnectorConfig dbzConfig;

    /**
     * Reads the schema of a table that neither the snapshot phase nor a DDL registered, or {@code
     * null} when the converter has no such source (unit tests). The connector never reconstructs a
     * schema from a canal message's own {@code mysqlType}: it names MySQL types only, without the
     * length or scale a Debezium {@link Table} needs, so a schema built from it would silently
     * describe the wrong columns.
     */
    @Nullable private final TableSchemaResolver tableSchemaResolver;

    public KafkaJsonRecordConverter(
            KafkaJsonRecordFactory factory, KafkaJsonSourceConfig sourceConfig) {
        this(factory, sourceConfig, null);
    }

    public KafkaJsonRecordConverter(
            KafkaJsonRecordFactory factory,
            KafkaJsonSourceConfig sourceConfig,
            @Nullable TableSchemaResolver tableSchemaResolver) {
        this.factory = factory;
        this.sourceConfig = sourceConfig;
        this.eventTimeMode = sourceConfig.getEventTime();
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
        this.tableSchemaResolver = tableSchemaResolver;
    }

    /**
     * Reads the schema of a table whose first data change arrived before any schema was registered
     * for it — the pure-stream case, where neither the snapshot phase nor the table's DDL was seen.
     *
     * <p>The production implementation reads the schema from the source database over JDBC and
     * registers it (see {@code KafkaJsonSourceFetchTaskContext}); a failure to do so must fail the
     * job rather than drop the rows, because a row that is dropped here cannot be recovered.
     */
    @FunctionalInterface
    public interface TableSchemaResolver {

        /**
         * Returns the registered schema of the table (or {@code null} when the table does not
         * exist), throwing when the source database cannot be read.
         */
        @Nullable
        Table resolve(TableId tableId);
    }

    /**
     * Returns the ordering event time (millis) of a message for the configured {@link EventTime}
     * mode, or {@code null} when the message carries no usable time. For a canal flatMessage it is
     * the binlog execution time {@code es} / the canal send time {@code ts} / the decoded TiDB
     * commit TSO ({@code _tidb.commitTs}, or {@code _tidb.watermarkTs} for a watermark event); for
     * a Debezium message it is {@code source.ts_ms} / {@code payload.ts_ms} / the decoded commit
     * TSO.
     */
    public static Long eventTime(KafkaJsonMessage message, EventTime eventTimeMode) {
        return message.getEventTimeValue(eventTimeMode);
    }

    /**
     * Converts one message into the data records it carries, dispatching on the message format.
     *
     * @param message the parsed message (not a DDL message; those are routed to the schema-change
     *     handler by the stream fetch task)
     * @param topic the Kafka topic the message was consumed from
     * @param partition the Kafka partition
     * @param kafkaOffset the Kafka partition-local offset
     * @return the emitted records; possibly empty (e.g. for DDL, watermarks or non-DML messages)
     */
    public List<SourceRecord> convert(
            KafkaJsonMessage message, String topic, int partition, long kafkaOffset) {
        switch (message.getFormat()) {
            case CANAL:
                return convertCanal((CanalMessage) message, topic, partition, kafkaOffset);
            case DEBEZIUM:
                return convertDebezium((DebeziumMessage) message, topic, partition, kafkaOffset);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported message format: " + message.getFormat());
        }
    }

    private List<SourceRecord> convertCanal(
            CanalMessage message, String topic, int partition, long kafkaOffset) {
        if (message.isDdl()) {
            return Collections.emptyList();
        }
        String op = message.getType() == null ? "" : message.getType().toUpperCase(Locale.ROOT);
        switch (op) {
            case "INSERT":
                return convertCanalRows(
                        message, topic, partition, kafkaOffset, Envelope.Operation.CREATE);
            case "UPDATE":
                return convertCanalRows(
                        message, topic, partition, kafkaOffset, Envelope.Operation.UPDATE);
            case "DELETE":
                return convertCanalRows(
                        message, topic, partition, kafkaOffset, Envelope.Operation.DELETE);
            case "TIDB_WATERMARK":
                // TiCDC emits these marker events (isDdl=false but type=TIDB_WATERMARK, data=null)
                // when the TiDB extension is enabled; they carry no DML rows and are not DDL.
                // TiCDC does not filter them itself, so dropping them is the consumer's job.
                return Collections.emptyList();
            default:
                // QUERY (isDdl=false: BEGIN/COMMIT/SET), GTID / XACOMPLETE / SAVEPOINT / ... :
                // no data rows
                return Collections.emptyList();
        }
    }

    /**
     * Converts one Debezium envelope message into its single data record. The {@code op} is already
     * in Debezium coding; the typed {@code before}/{@code after} images are converted against the
     * registered table schema.
     */
    private List<SourceRecord> convertDebezium(
            DebeziumMessage message, String topic, int partition, long kafkaOffset) {
        if (message.getMessageType() != KafkaJsonMessage.MessageType.DML) {
            // DDL messages are routed to the schema-change handler by the stream fetch task;
            // watermarks ({@code op=m}) and unknown ops carry no rows
            return Collections.emptyList();
        }
        DebeziumMessage.Payload payload = message.getPayload();
        if (payload == null) {
            return Collections.emptyList();
        }
        Envelope.Operation op = toEnvelopeOp(payload.getOp());
        if (op == null) {
            return Collections.emptyList();
        }
        Table table = resolveTable(message);
        if (table == null) {
            // the table is outside the configured table filter (logged by resolveTable)
            return Collections.emptyList();
        }
        Long eventTimeValue = eventTime(message, eventTimeMode);
        long eventTime = eventTimeValue == null ? -1L : eventTimeValue;
        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
                        dbzConfig,
                        message.getDatabase(),
                        message.getTable(),
                        eventTime,
                        message.getEs(),
                        message.getTs(),
                        SnapshotRecord.FALSE);
        JsonNode before = payload.getBefore();
        JsonNode after = payload.getAfter();
        Object[] beforeData = before == null ? null : factory.debeziumRowData(table, before);
        Object[] afterData = after == null ? null : factory.debeziumRowData(table, after);
        return Collections.singletonList(
                factory.createRecord(
                        table,
                        beforeData,
                        afterData,
                        op,
                        sourceInfo,
                        topic,
                        partition,
                        kafkaOffset));
    }

    private static Envelope.Operation toEnvelopeOp(String op) {
        if (op == null) {
            return null;
        }
        switch (op) {
            case "c":
                return Envelope.Operation.CREATE;
            case "r":
                return Envelope.Operation.READ;
            case "u":
                return Envelope.Operation.UPDATE;
            case "d":
                return Envelope.Operation.DELETE;
            default:
                return null;
        }
    }

    private List<SourceRecord> convertCanalRows(
            CanalMessage message,
            String topic,
            int partition,
            long kafkaOffset,
            Envelope.Operation op) {
        Table table = resolveTable(message);
        if (table == null) {
            return Collections.emptyList();
        }
        Long eventTimeValue = eventTime(message, eventTimeMode);
        long eventTime = eventTimeValue == null ? -1L : eventTimeValue;
        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
                        dbzConfig,
                        message.getDatabase(),
                        message.getTable(),
                        eventTime,
                        message.getEs(),
                        message.getTs(),
                        SnapshotRecord.FALSE);

        List<Map<String, String>> data = message.getData();
        List<Map<String, String>> old = message.getOld();
        List<SourceRecord> records = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++) {
            Map<String, String> afterRow = data.get(i);
            Map<String, String> beforeRow = old.size() > i ? old.get(i) : null;
            if (op == Envelope.Operation.DELETE) {
                // canal puts the (before) row into data for DELETE
                records.add(
                        factory.createRecord(
                                table,
                                factory.canalRowData(table, afterRow),
                                null,
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            } else if (op == Envelope.Operation.UPDATE) {
                records.add(
                        factory.createRecord(
                                table,
                                beforeRow == null
                                        ? null
                                        : factory.canalRowData(
                                                table, completeBeforeRow(afterRow, beforeRow)),
                                factory.canalRowData(table, afterRow),
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            } else {
                records.add(
                        factory.createRecord(
                                table,
                                null,
                                factory.canalRowData(table, afterRow),
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            }
        }
        return records;
    }

    /**
     * Reconstructs the complete before row of an {@code UPDATE}.
     *
     * <p>In the canal flatMessage the {@code data} array carries the full row <em>after</em> the
     * change, while the {@code old} array carries only the columns that actually changed. A column
     * that did not change has the same value before and after the {@code UPDATE}, so the before
     * image is simply the after row with the changed columns overlaid with their {@code old}
     * values: {@code complete = afterRow ∪ old}.
     *
     * <p>This is not cosmetic: the reconstructed row is what the pipeline exposes to the consumer.
     * It flows into the Debezium envelope's {@code before} struct of the {@code SourceRecord} (see
     * {@link KafkaJsonRecordFactory#createRecord}), and the pipeline deserializer reads it as the
     * {@code DataChangeEvent.before()} image of the emitted update event. Without the merge the
     * unchanged columns would be {@code null} here, and the schema converters would fill their NOT
     * NULL schema defaults ({@code 0}/{@code ""}) when building the struct, so the consumer would
     * receive a fabricated before row instead of the true old row.
     */
    private static Map<String, String> completeBeforeRow(
            Map<String, String> afterRow, Map<String, String> beforeRow) {
        Map<String, String> complete = new HashMap<>(afterRow);
        complete.putAll(beforeRow);
        return complete;
    }

    /**
     * Returns the registered schema of the table a message belongs to, or {@code null} when the
     * message belongs to a table outside the configured table filter (the caller drops it).
     *
     * <p>A table is registered by the schema the snapshot phase discovered for it, by its {@code
     * CREATE} DDL, or by a rename of it. A table inside the filter with no registered schema — its
     * first change arrives before any DDL in a pure-stream deployment — is read from the source
     * database through {@link TableSchemaResolver} and so registered. If that read fails, the job
     * fails: a row dropped here cannot be recovered.
     */
    @Nullable
    private Table resolveTable(CanalMessage message) {
        return resolveTable(message.getDatabase(), message.getTable());
    }

    /**
     * Returns the schema of the table a Debezium message belongs to. A Debezium message carries no
     * table metadata at all, so a table that the snapshot phase and any DDL missed is resolved the
     * same way as a canal table.
     */
    @Nullable
    private Table resolveTable(DebeziumMessage message) {
        return resolveTable(message.getDatabase(), message.getTable());
    }

    @Nullable
    private Table resolveTable(String database, String tableName) {
        TableId tableId = new TableId(database == null ? "" : database, null, tableName);
        Table table = factory.tableFor(tableId);
        if (table != null) {
            return table;
        }
        if (!sourceConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            // The stream carries every table the upstream log covers; a table outside the include
            // list was never meant to be captured, so its changes are dropped rather than resolved.
            LOG.debug("Ignoring a data change of {}: outside the configured table filter", tableId);
            return null;
        }
        if (tableSchemaResolver == null) {
            throw failFast(database, tableName, "no schema source is configured to read it", null);
        }
        LOG.info(
                "No registered schema for {}.{}; reading it from the source database over JDBC",
                database,
                tableName);
        Table resolved;
        try {
            resolved = tableSchemaResolver.resolve(tableId);
        } catch (RuntimeException e) {
            throw failFast(
                    database,
                    tableName,
                    "reading it from the source database over JDBC failed: " + e.getMessage(),
                    e);
        }
        if (resolved == null) {
            throw failFast(database, tableName, "the source database has no such table", null);
        }
        return resolved;
    }

    /**
     * Builds the failure raised when the first data change of a table arrives with no schema
     * registered. Dropping the change would lose data silently, so the job stops with a message
     * that names the table and the likely causes.
     */
    private static FlinkRuntimeException failFast(
            String database, String tableName, String reason, @Nullable Throwable cause) {
        String message =
                "Failed to resolve the schema of "
                        + database
                        + "."
                        + tableName
                        + " after consuming its first data change and finding no registered "
                        + "schema: "
                        + reason
                        + ". The schema of a table is registered by the JDBC snapshot or by its "
                        + "CREATE/ALTER DDL; in a pure-stream deployment (no snapshot, no DDL) the "
                        + "connector reads it from the source database instead. Make sure that "
                        + "database is reachable with the configured connection and that "
                        + tableName
                        + " exists and is included in the configured table filter.";
        return cause == null
                ? new FlinkRuntimeException(message)
                : new FlinkRuntimeException(message, cause);
    }
}
