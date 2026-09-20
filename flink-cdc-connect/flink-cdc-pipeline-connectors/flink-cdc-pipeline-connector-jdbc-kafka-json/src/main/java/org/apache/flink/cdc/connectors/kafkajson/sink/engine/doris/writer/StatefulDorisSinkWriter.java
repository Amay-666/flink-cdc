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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisRowConverter;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisWriteMetrics;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The stateful counterpart of {@link DorisSinkWriter}: same buffering, conversion and flush
 * behaviour, but the subtask checkpoints {@link DorisWriterState} and resumes from it.
 *
 * <p>The two writers exist side by side rather than one replacing the other, because they differ in
 * what a checkpoint means. {@link DorisSinkWriter} is the released behaviour and stays untouched;
 * {@code sink.writer=stateful} selects this one, whose rows are visible as soon as a flush returns
 * exactly as before, with two differences that only show across a restart:
 *
 * <ul>
 *   <li>the Group Commit sequence counter is checkpointed, so a restored subtask continues above
 *       the values it already wrote instead of trusting that the wall clock moved forward. Both
 *       hold normally; only the state holds when the clock does not.
 *   <li>the writer is a {@link StatefulSinkWriter}, which is what lets {@code
 *       sink.writer=stateful-2pc} build on it: two-phase commit needs a checkpoint-aligned writer,
 *       since it is the checkpoint that decides when a batch becomes visible.
 * </ul>
 *
 * <p>Nothing about the buffered rows needs checkpointing, which is worth stating because it looks
 * like it should. Flink calls {@code flush(false)} before every checkpoint's state is taken, so the
 * buffers are empty at exactly the moments that matter, and a restarted job re-reads the rows after
 * the checkpoint from the source rather than from its own memory. What a restart must not lose is
 * therefore the counter and, in two-phase mode, the transactions — not the queues.
 *
 * <p>See {@link DorisSinkWriter} for the write protocol itself (upserts keyed by primary key, the
 * delete marker, Group Commit sequence values, the flush thresholds and the periodic flush).
 */
public class StatefulDorisSinkWriter implements StatefulSinkWriter<Event, DorisWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulDorisSinkWriter.class);

    protected final DorisDataSinkOptions options;
    protected final DorisHttpClient httpClient;
    protected final DorisWriteMetrics metrics;

    /** Prefix of every label this writer generates. */
    protected final String labelPrefix;

    private final ZoneId pipelineZoneId;

    /** Latest schema per table, evolved by the schema-change events flowing downstream. */
    private final Map<TableId, Schema> schemaMaps = new HashMap<>();

    private final Map<TableId, DorisRowConverter> rowConverters = new HashMap<>();

    /**
     * Per-table FIFO queues of rows awaiting StreamLoad. Upserts and deletes share one queue so a
     * single batch preserves arrival order; delete rows carry {@link
     * DorisHttpClient#DELETE_SIGN_COLUMN} set to {@code true}.
     */
    private final Map<TableId, ArrayDeque<Map<String, Object>>> buffer = new HashMap<>();

    /** Total rows across all table queues, for the bounded global cache. */
    private int bufferedRows;

    private final boolean isGroupCommit;

    /** Name of the sequence column, or {@code null} when Group Commit is disabled. */
    private final String sequenceColumnName;

    /**
     * Monotonically increasing sequence counter, one per subtask. Initialised to {@code
     * System.currentTimeMillis() * 1_000_000} so that post-restart values are always larger than
     * pre-restart values (the wall clock has advanced) — and raised to at least one above the
     * checkpointed value on restore, so the guarantee survives a clock that does not advance.
     */
    private final AtomicLong sequenceCounter =
            new AtomicLong(System.currentTimeMillis() * 1_000_000L);

    private volatile boolean closed;
    private ScheduledFuture<?> flushTimer;

    protected StatefulDorisSinkWriter(
            DorisDataSinkOptions options, ZoneId pipelineZoneId, Sink.InitContext initContext) {
        this.options = options;
        this.pipelineZoneId = pipelineZoneId;
        this.metrics = new DorisWriteMetrics(initContext.metricGroup());
        this.isGroupCommit = options.isGroupCommitEnabled();
        this.sequenceColumnName =
                options.isGroupCommitEnabled() ? options.getSequenceColumnName() : null;
        this.labelPrefix = options.getLabelPrefix();
        if (this.isGroupCommit) {
            Preconditions.checkArgument(
                    !Preconditions.checkNotNull(sequenceColumnName).isEmpty(),
                    "Sequence column name must not be blank when group commit is enabled.");
        }
        this.httpClient =
                new DorisHttpClient(
                        options.getFenodes(),
                        options.getUsername(),
                        options.getPassword(),
                        options.getMaxRetries(),
                        options.getStreamLoadProperties(),
                        options.getGroupCommitMode().headerValue());
        schedulePeriodicFlush(initContext);
    }

    @Override
    public void write(Event element, SinkWriter.Context context) throws IOException {
        if (element instanceof DataChangeEvent) {
            writeDataChangeEvent((DataChangeEvent) element);
        } else if (element instanceof SchemaChangeEvent) {
            applySchemaChange((SchemaChangeEvent) element);
        }
        // FlushEvent never reaches the writer: DataSinkWriterOperator intercepts it and calls
        // flush() directly.
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        metrics.recordFlush();
        flushBuffered();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (flushTimer != null) {
            flushTimer.cancel(false);
        }
        if (shouldFlushOnClose()) {
            flushBuffered();
        }
        httpClient.close();
    }

    /**
     * Whether {@link #close()} still flushes the buffer. True for the single-phase writer, where an
     * extra load only makes rows visible sooner. False for the two-phase writer, where it would
     * open a transaction that nothing will ever commit: the committer only commits what a
     * checkpoint emitted, and a subtask being closed is not taking a checkpoint. Those rows are not
     * lost — a restarted job re-reads them from the source.
     */
    protected boolean shouldFlushOnClose() {
        return true;
    }

    /**
     * Sends one drained batch of rows to Doris, immediately visible.
     *
     * <p>The rows have already been removed from the buffer, so throwing leaves them to the source
     * replay rather than to a retry of this call. Called for every flush — the one before a
     * checkpoint barrier, the buffer threshold, the periodic one — which is why {@link
     * TwoPhaseDorisSinkWriter} overrides it: several batches can arrive between two checkpoints, so
     * its labels have to distinguish them.
     */
    protected void landBatch(String database, String table, List<Map<String, Object>> rows)
            throws IOException {
        // Group Commit mode: pass null label so DorisHttpClient sends the group_commit header
        // instead of a label (specifying a label degrades to non-Group-Commit).
        streamLoad(database, table, isGroupCommit ? null : newLabel(database, table), rows);
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(new DorisWriterState(labelPrefix, sequenceCounter.get()));
    }

    /**
     * Restores this subtask from the states Flink redistributed to it.
     *
     * <p>Flink's list state has no partitioner, so a rescale can hand a new subtask several old
     * subtasks' states (or, when the parallelism shrinks, one subtask's state to one reader). The
     * counter is therefore taken as the maximum over all of them: a value below one already written
     * would let a late-arriving row carry a smaller Group Commit sequence than the row it should
     * overwrite, which is exactly the ordering Group Commit relies on.
     */
    public void restoreState(Collection<DorisWriterState> states) {
        long highestRestored = 0L;
        for (DorisWriterState state : states) {
            if (!labelPrefix.equals(state.getLabelPrefix())) {
                LOG.warn(
                        "Restoring Doris sink state written under label prefix '{}' while '{}' is"
                                + " configured. Labels of a checkpoint that is still open are found"
                                + " through the label this writer is about to reuse, so the restore"
                                + " is unaffected; this is reported because a prefix that was shared"
                                + " with another job is the one case where writes can collide.",
                        state.getLabelPrefix(),
                        labelPrefix);
            }
            highestRestored = Math.max(highestRestored, state.getSequenceCounter());
        }
        // Both halves matter: one above the checkpointed value keeps the counter increasing across
        // the restart, and the wall-clock base keeps it above anything written by a life whose
        // state
        // is not what we restored from (an older savepoint, or a job that wrote with a counter the
        // state never captured).
        sequenceCounter.set(Math.max(highestRestored + 1, System.currentTimeMillis() * 1_000_000L));
    }

    private void writeDataChangeEvent(DataChangeEvent event) throws IOException {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        DorisRowConverter converter = rowConverters.get(tableId);
        if (schema == null || converter == null) {
            throw new IOException(
                    "Received a data change event for unknown table "
                            + tableId
                            + "; the CreateTableEvent must precede its data.");
        }
        Map<String, Object> row;
        if (event.op() == OperationType.DELETE) {
            // Deletes are keyed by their pre-image. With batch delete enabled the row carries the
            // delete-sign marker and the MERGE batch removes it; with it disabled the pre-image is
            // upserted instead (the row is kept, not removed).
            row = converter.convert(event.before(), schema);
            row.put(DorisHttpClient.DELETE_SIGN_COLUMN, options.isEnableBatchDelete());
        } else {
            // INSERT / UPDATE / REPLACE are all upserts in the Doris UNIQUE model.
            row = converter.convert(event.after(), schema);
            row.put(DorisHttpClient.DELETE_SIGN_COLUMN, false);
        }
        // Group Commit: inject a monotonically increasing sequence value. The same value goes on
        // both upserts and deletes so that a late-arriving delete (larger seq) can correctly
        // override an earlier upsert (smaller seq) even when Doris reorders batch commits.
        if (sequenceColumnName != null) {
            row.put(sequenceColumnName, sequenceCounter.getAndIncrement());
        }
        buffer.computeIfAbsent(tableId, t -> new ArrayDeque<>()).add(row);
        bufferedRows++;
        metrics.recordWriteRow();
        if (buffer.get(tableId).size() >= options.getBufferSize()) {
            flushTable(tableId);
        }
        // Bounded global cache: per-table thresholds alone can let buffered rows grow without
        // limit once the pipeline tracks many tables, so spill the largest table queue whenever
        // the total row count passes the configured cap.
        while (bufferedRows > options.getMaxBufferedRows()) {
            flushLargestTable();
        }
        metrics.setBufferedRows(bufferedRows);
    }

    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            CreateTableEvent create = (CreateTableEvent) event;
            schemaMaps.put(create.tableId(), create.getSchema());
            rowConverters.put(
                    create.tableId(), new DorisRowConverter(create.getSchema(), pipelineZoneId));
        } else if (event instanceof RenameTableEvent) {
            RenameTableEvent rename = (RenameTableEvent) event;
            // Subsequent data carries the new table ids: re-key the local view. Every source key is
            // read out before any target key is written — an `a TO b, b TO a` swap moves both
            // names, and in-place moves would have the second pair read back what the first wrote.
            //
            // Moving the buffered rows is defensive: the blocking protocol flushes the writer
            // before the rename DDL is applied, so the buffers are normally empty here.
            List<RenameTableEvent.TableRename> pairs = rename.getPairs();
            Map<TableId, ArrayDeque<Map<String, Object>>> movedBuffers = new HashMap<>();
            for (RenameTableEvent.TableRename pair : pairs) {
                schemaMaps.remove(pair.getOldTableId());
                rowConverters.remove(pair.getOldTableId());
                ArrayDeque<Map<String, Object>> rows = buffer.remove(pair.getOldTableId());
                if (rows != null) {
                    movedBuffers.put(pair.getNewTableId(), rows);
                }
            }
            buffer.putAll(movedBuffers);
            for (RenameTableEvent.TableRename pair : pairs) {
                schemaMaps.put(pair.getNewTableId(), pair.getSchema());
                rowConverters.put(
                        pair.getNewTableId(),
                        new DorisRowConverter(pair.getSchema(), pipelineZoneId));
            }
        } else if (event instanceof DropTableEvent) {
            ArrayDeque<Map<String, Object>> dropped = buffer.remove(event.tableId());
            if (dropped != null) {
                bufferedRows -= dropped.size();
                metrics.setBufferedRows(bufferedRows);
            }
            schemaMaps.remove(event.tableId());
            rowConverters.remove(event.tableId());
        } else if (event instanceof TruncateTableEvent) {
            // The blocking protocol flushes all buffered rows before the truncate DDL is applied
            // and no data flows until the truncate event has been broadcast downstream, so the
            // writer keeps its schema and has nothing to do.
        } else if (event instanceof AlterTableCommentEvent
                || event instanceof AlterColumnCommentEvent) {
            // Comments do not affect row conversion; keep the current schema and converter.
        } else {
            Schema current = schemaMaps.get(event.tableId());
            if (current != null) {
                Schema evolved = SchemaUtils.applySchemaChangeEvent(current, event);
                schemaMaps.put(event.tableId(), evolved);
                rowConverters.put(event.tableId(), new DorisRowConverter(evolved, pipelineZoneId));
            }
        }
    }

    private void flushBuffered() throws IOException {
        for (TableId tableId : new HashSet<>(buffer.keySet())) {
            flushTable(tableId);
        }
    }

    private void flushLargestTable() throws IOException {
        TableId largest = null;
        int largestSize = -1;
        for (Map.Entry<TableId, ArrayDeque<Map<String, Object>>> entry : buffer.entrySet()) {
            int size = entry.getValue().size();
            if (size > largestSize) {
                largestSize = size;
                largest = entry.getKey();
            }
        }
        if (largest != null) {
            flushTable(largest);
        }
    }

    private void flushTable(TableId tableId) throws IOException {
        ArrayDeque<Map<String, Object>> rows = buffer.remove(tableId);
        if (rows == null || rows.isEmpty()) {
            return;
        }
        bufferedRows -= rows.size();
        landBatch(options.mapDatabase(tableId), options.mapTable(tableId), drain(rows));
        metrics.setBufferedRows(bufferedRows);
    }

    /** Single-phase landing: one StreamLoad, visible as soon as it returns. */
    protected void streamLoad(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        try {
            int bytes = httpClient.streamLoad(database, table, label, rows);
            metrics.recordStreamLoad(bytes);
        } catch (IOException e) {
            metrics.recordStreamLoadFailure();
            throw e;
        }
    }

    /**
     * The label of one single-phase batch: unique per load, so a retry is deduplicated by Doris.
     */
    protected String newLabel(String database, String table) {
        return labelPrefix
                + "_"
                + sanitize(database)
                + "_"
                + sanitize(table)
                + "_"
                + UUID.randomUUID().toString().replace("-", "");
    }

    /** Doris labels and table names are limited to characters a URL path and a header accept. */
    protected static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    protected static List<Map<String, Object>> drain(ArrayDeque<Map<String, Object>> rows) {
        List<Map<String, Object>> batch = new ArrayList<>(rows.size());
        Map<String, Object> row;
        while ((row = rows.poll()) != null) {
            batch.add(row);
        }
        return batch;
    }

    private void schedulePeriodicFlush(Sink.InitContext initContext) {
        if (options.getFlushInterval().isZero() || options.getFlushInterval().isNegative()) {
            return;
        }
        ProcessingTimeService timeService = initContext.getProcessingTimeService();
        flushTimer =
                timeService.registerTimer(
                        timeService.getCurrentProcessingTime()
                                + options.getFlushInterval().toMillis(),
                        timestamp -> {
                            if (closed) {
                                return;
                            }
                            try {
                                flushBuffered();
                            } catch (IOException e) {
                                // Log before propagating: this runs on the timer thread, so the
                                // exception is reported from the processing-time service rather
                                // than from this writer, and it says nothing about which batch is
                                // lost.
                                LOG.error(
                                        "Periodic flush of the Doris buffer failed; failing the "
                                                + "task instead of dropping the buffered rows.",
                                        e);
                                throw new RuntimeException("Failed to flush Doris buffer.", e);
                            }
                            schedulePeriodicFlush(initContext);
                        });
    }
}
