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

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonSchema;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.convert.MySqlValueConverters;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonQueryUtils;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.List;

/**
 * The snapshot fetch task of the kafka-json source, driving the incremental snapshot of one
 * snapshot split through the base framework's {@link AbstractScanFetchTask} algorithm:
 *
 * <ol>
 *   <li>the base captures the low watermark (the current Kafka stream position),
 *   <li>{@link #executeDataSnapshot} reads the split rows from MySQL via JDBC and enqueues them as
 *       {@link Envelope.Operation#READ} records,
 *   <li>the base captures the high watermark and, if the two differ, asks {@link
 *       #executeBackfillTask} to replay the change log between them (reusing the shared {@link
 *       KafkaJsonStreamFetchTask} on the same Kafka consumer),
 *   <li>the base dispatches the end watermark that finalizes the split.
 * </ol>
 */
public class KafkaJsonScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonScanFetchTask.class);

    public KafkaJsonScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        KafkaJsonSourceFetchTaskContext sourceFetchContext =
                (KafkaJsonSourceFetchTaskContext) context;
        KafkaJsonSnapshotSplitReadTask snapshotSplitReadTask =
                new KafkaJsonSnapshotSplitReadTask(
                        sourceFetchContext.getRecordFactory(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getSourceConfig(),
                        sourceFetchContext.getQueue(),
                        snapshotSplit);
        snapshotSplitReadTask.execute(new KafkaJsonSnapshotSplitChangeEventSourceContext());
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        // The backfill is a bounded stream read between the low and high watermark, exactly as the
        // Postgres connector reuses its stream reader for the backfill of each snapshot split. The
        // stream task consumes from the same Kafka consumer shared per reader, so no second
        // consumer is opened for the backfill phase.
        new KafkaJsonStreamFetchTask(backfillStreamSplit)
                .execute((KafkaJsonSourceFetchTaskContext) context);
    }

    /**
     * Reads the rows of a snapshot split from MySQL via JDBC and enqueues them as Debezium-shaped
     * {@code READ} records through the shared {@link KafkaJsonRecordFactory}.
     *
     * <p>The rows are taken straight from the JDBC result set and fed to the {@code
     * valueFromColumnData} converters of the table schema, mirroring the Debezium MySQL snapshot
     * reader. The position of a snapshot record is irrelevant to the incremental-snapshot algorithm
     * — only its key and envelope shape matter — so each split is stamped with the wall-clock time
     * when it is read.
     */
    public static class KafkaJsonSnapshotSplitReadTask {

        private static final Logger LOG =
                LoggerFactory.getLogger(KafkaJsonSnapshotSplitReadTask.class);
        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final KafkaJsonRecordFactory recordFactory;
        private final KafkaJsonSchema databaseSchema;
        private final JdbcConnection jdbcConnection;
        private final MySqlConnectorConfig connectorConfig;
        private final KafkaJsonSourceConfig sourceConfig;
        private final ChangeEventQueue<DataChangeEvent> queue;
        private final SnapshotSplit snapshotSplit;

        public KafkaJsonSnapshotSplitReadTask(
                KafkaJsonRecordFactory recordFactory,
                KafkaJsonSchema databaseSchema,
                JdbcConnection jdbcConnection,
                MySqlConnectorConfig connectorConfig,
                KafkaJsonSourceConfig sourceConfig,
                ChangeEventQueue<DataChangeEvent> queue,
                SnapshotSplit snapshotSplit) {
            this.recordFactory = recordFactory;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.connectorConfig = connectorConfig;
            this.sourceConfig = sourceConfig;
            this.queue = queue;
            this.snapshotSplit = snapshotSplit;
        }

        public void execute(ChangeEventSource.ChangeEventSourceContext context)
                throws SQLException, InterruptedException {
            TableId tableId = snapshotSplit.getTableId();
            Table table = databaseSchema.tableFor(tableId);
            if (table == null) {
                throw new IllegalStateException(
                        "Cannot find table "
                                + tableId
                                + " in the kafka-json schema; the table schema must be registered "
                                + "before reading its snapshot");
            }

            // snapshot records carry the wall-clock time when the split is read, mirroring the
            // Debezium snapshot emitter; the record position is never compared against the
            // watermarks (only the key and the envelope shape matter)
            long snapshotEventTime = System.currentTimeMillis();
            KafkaJsonSourceInfo snapshotSourceInfo =
                    new KafkaJsonSourceInfo(
                            connectorConfig,
                            tableId.catalog(),
                            tableId.table(),
                            snapshotEventTime,
                            snapshotEventTime,
                            snapshotEventTime,
                            SnapshotRecord.TRUE);
            List<String> kafkaTopics = sourceConfig.getKafkaTopics();
            if (kafkaTopics.isEmpty()) {
                throw new IllegalStateException(
                        "No Kafka topic configured for the kafka-json source");
            }
            final String topic = kafkaTopics.get(0);

            final String selectSql =
                    KafkaJsonQueryUtils.buildSplitScanQuery(
                            tableId,
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    tableId,
                    selectSql);

            long exportStart = System.currentTimeMillis();
            try (PreparedStatement selectStatement =
                            KafkaJsonQueryUtils.readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    connectorConfig.getQueryFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                List<Column> columns = table.columns();
                long rows = 0;
                long lastLogTime = exportStart;
                while (context.isRunning() && rs.next()) {
                    rows++;
                    final Object[] row = new Object[columns.size()];
                    for (Column column : columns) {
                        row[column.position() - 1] = readField(rs, column.position(), column);
                    }
                    SourceRecord record =
                            recordFactory.createRecord(
                                    table,
                                    null,
                                    row,
                                    Envelope.Operation.READ,
                                    snapshotSourceInfo,
                                    topic,
                                    0,
                                    0L);
                    queue.enqueue(new DataChangeEvent(record));

                    long now = System.currentTimeMillis();
                    if (now - lastLogTime >= LOG_INTERVAL.toMillis()) {
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(now - exportStart));
                        lastLogTime = now;
                    }
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(System.currentTimeMillis() - exportStart));
            }
        }

        /**
         * Reads a JDBC field through the type-specific accessor — the DBZ-3238 workaround the MySQL
         * connector's snapshot read task ({@code MySqlSnapshotSplitReadTask#readField}) applies. A
         * raw {@code rs.getObject()} returns Java types the Debezium value converters reject:
         * {@code java.sql.Time} for TIME (DBZ-342) and a {@code Boolean} (TINYINT(1)) or {@code
         * Short} (unsigned) for TINYINT — the conversion then nulls the value.
         */
        private Object readField(ResultSet rs, int fieldNo, Column column) throws SQLException {
            if (column.jdbcType() == Types.TIME) {
                return readTimeField(rs, fieldNo);
            }
            if (column.jdbcType() == Types.TINYINT || column.jdbcType() == Types.SMALLINT) {
                // JDBC's rs.getObject() will return a Boolean for all TINYINT(1) columns and a
                // Short for an unsigned TINYINT; read both as int
                return rs.getObject(fieldNo) == null ? null : rs.getInt(fieldNo);
            }
            return rs.getObject(fieldNo);
        }

        /**
         * Reads a MySQL TIME field into the {@link java.time.Duration} the value converters expect.
         *
         * <p>The pool's JDBC URL ({@code useCursorFetch=true}) serves snapshot rows through the
         * binary protocol, whose TIME bytes are the packed {@code ProtocolBinary:: MYSQL_TYPE_TIME}
         * layout — sign flag, days, hours, minutes, seconds, optional microseconds — not the {@code
         * HH:mm:ss[.ffffff]} text that {@code MySqlValueConverters#stringToDuration} (the DBZ-342
         * workaround the MySQL connector's text-protocol reader uses) expects. The sign flag (0 or
         * 1) is never a printable digit, so the two encodings are unambiguous.
         */
        private Object readTimeField(ResultSet rs, int fieldNo) throws SQLException {
            Blob b = rs.getBlob(fieldNo);
            if (b == null) {
                return null; // Don't continue parsing time field if it is null
            }
            byte[] bytes = b.getBytes(1, (int) b.length());
            if (bytes.length == 0) {
                return null;
            }
            if (bytes[0] == 0 || bytes[0] == 1) {
                return binaryTimeToDuration(bytes);
            }
            return MySqlValueConverters.stringToDuration(new String(bytes, StandardCharsets.UTF_8));
        }
    }

    /**
     * Parses the packed {@code ProtocolBinary::MYSQL_TYPE_TIME} layout: a sign flag, an unsigned
     * 4-byte little-endian day count, hours, minutes, seconds and an optional unsigned 4-byte
     * little-endian microsecond part (8 bytes without the fraction, 12 with). Visible for testing.
     */
    static Duration binaryTimeToDuration(byte[] bytes) {
        if (bytes.length != 8 && bytes.length != 12) {
            throw new IllegalStateException(
                    "Invalid byte length of a MySQL binary TIME value: " + bytes.length);
        }
        boolean negative = bytes[0] == 1;
        int days = readUnsignedIntLE(bytes, 1);
        int hours = bytes[5] & 0xFF;
        int minutes = bytes[6] & 0xFF;
        int seconds = bytes[7] & 0xFF;
        long micros = bytes.length == 12 ? readUnsignedIntLE(bytes, 8) : 0L;
        Duration duration =
                Duration.ofSeconds((((long) days) * 24 + hours) * 3600L + minutes * 60L + seconds)
                        .plusNanos(micros * 1000L);
        return negative ? duration.negated() : duration;
    }

    private static int readUnsignedIntLE(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | ((bytes[offset + 3] & 0xFF) << 24);
    }

    /** The {@link ChangeEventSource.ChangeEventSourceContext} of a snapshot split read. */
    private class KafkaJsonSnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
