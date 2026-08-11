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

package org.apache.flink.cdc.connectors.canal.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.canal.source.CanalSchema;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.message.CanalRecordFactory;
import org.apache.flink.cdc.connectors.canal.source.schema.CanalSourceInfo;
import org.apache.flink.cdc.connectors.canal.source.utils.CanalQueryUtils;

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

/**
 * The snapshot fetch task of the Canal source, driving the incremental snapshot of one snapshot
 * split through the base framework's {@link AbstractScanFetchTask} algorithm:
 *
 * <ol>
 *   <li>the base captures the low watermark (the current Kafka stream position),
 *   <li>{@link #executeDataSnapshot} reads the split rows from MySQL via JDBC and enqueues them as
 *       {@link Envelope.Operation#READ} records,
 *   <li>the base captures the high watermark and, if the two differ, asks {@link
 *       #executeBackfillTask} to replay the change log between them (reusing the shared {@link
 *       CanalStreamFetchTask} on the same Kafka consumer),
 *   <li>the base dispatches the end watermark that finalizes the split.
 * </ol>
 */
public class CanalScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(CanalScanFetchTask.class);

    public CanalScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        CanalSourceFetchTaskContext sourceFetchContext = (CanalSourceFetchTaskContext) context;
        CanalSnapshotSplitReadTask snapshotSplitReadTask =
                new CanalSnapshotSplitReadTask(
                        sourceFetchContext.getRecordFactory(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getSourceConfig(),
                        sourceFetchContext.getQueue(),
                        snapshotSplit);
        snapshotSplitReadTask.execute(new CanalSnapshotSplitChangeEventSourceContext());
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        // The backfill is a bounded stream read between the low and high watermark, exactly as the
        // Postgres connector reuses its stream reader for the backfill of each snapshot split. The
        // stream task consumes from the same Kafka consumer shared per reader, so no second
        // consumer is opened for the backfill phase.
        new CanalStreamFetchTask(backfillStreamSplit).execute((CanalSourceFetchTaskContext) context);
    }

    /**
     * Reads the rows of a snapshot split from MySQL via JDBC and enqueues them as Debezium-shaped
     * {@code READ} records through the shared {@link CanalRecordFactory}.
     *
     * <p>The rows are taken straight from the JDBC result set and fed to the {@code
     * valueFromColumnData} converters of the table schema, mirroring the Debezium MySQL snapshot
     * reader. The position of a snapshot record is irrelevant to the incremental-snapshot
     * algorithm — only its key and envelope shape matter — so each split is stamped with the
     * wall-clock time when it is read.
     */
    public static class CanalSnapshotSplitReadTask {

        private static final Logger LOG = LoggerFactory.getLogger(CanalSnapshotSplitReadTask.class);
        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final CanalRecordFactory recordFactory;
        private final CanalSchema databaseSchema;
        private final JdbcConnection jdbcConnection;
        private final MySqlConnectorConfig connectorConfig;
        private final CanalSourceConfig sourceConfig;
        private final ChangeEventQueue<DataChangeEvent> queue;
        private final SnapshotSplit snapshotSplit;

        public CanalSnapshotSplitReadTask(
                CanalRecordFactory recordFactory,
                CanalSchema databaseSchema,
                JdbcConnection jdbcConnection,
                MySqlConnectorConfig connectorConfig,
                CanalSourceConfig sourceConfig,
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
                                + " in the canal schema; the table schema must be registered "
                                + "before reading its snapshot");
            }

            // snapshot records carry the wall-clock time when the split is read, mirroring the
            // Debezium snapshot emitter; the record position is never compared against the
            // watermarks (only the key and the envelope shape matter)
            long snapshotEventTime = System.currentTimeMillis();
            CanalSourceInfo snapshotSourceInfo =
                    new CanalSourceInfo(
                            connectorConfig,
                            tableId.catalog(),
                            tableId.table(),
                            snapshotEventTime,
                            snapshotEventTime,
                            snapshotEventTime,
                            SnapshotRecord.TRUE);
            List<String> kafkaTopics = sourceConfig.getKafkaTopics();
            if (kafkaTopics.isEmpty()) {
                throw new IllegalStateException("No Kafka topic configured for the Canal source");
            }
            final String topic = kafkaTopics.get(0);

            final String selectSql =
                    CanalQueryUtils.buildSplitScanQuery(
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
                            CanalQueryUtils.readTableSplitDataStatement(
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
                    for (int i = 0; i < columns.size(); i++) {
                        Column column = columns.get(i);
                        row[column.position() - 1] = rs.getObject(column.position());
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
    }

    /** The {@link ChangeEventSource.ChangeEventSourceContext} of a snapshot split read. */
    private class CanalSnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
