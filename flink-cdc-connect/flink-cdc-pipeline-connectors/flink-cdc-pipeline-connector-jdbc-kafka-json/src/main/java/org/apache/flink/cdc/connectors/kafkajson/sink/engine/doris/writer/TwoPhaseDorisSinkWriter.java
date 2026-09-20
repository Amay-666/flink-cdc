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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittable;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommitter;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * The Doris writer that runs every batch through Stream Load's two-phase commit: rows are written
 * when they are flushed and become visible when the checkpoint they belong to completes.
 *
 * <p>It is {@link StatefulDorisSinkWriter} with one method replaced — {@link #landBatch}
 * pre-commits instead of loading — plus the bookkeeping that follows from it. That bookkeeping is
 * one thing: which transactions the current checkpoint has open, so {@link #prepareCommit()} can
 * hand them to the {@link DorisCommitter}. Everything else about batching, buffering and schema
 * evolution is inherited unchanged.
 *
 * <p>Labels are the delicate part, because a two-phase load cannot retry under a fresh label the
 * way the single-phase writer does (that writer's labels are random UUIDs and its retries lean on
 * Doris deduplicating the same label). Here a label has to be <em>derivable</em>, since it is what
 * says which transaction a batch belongs to when a job restarts, so it is built out of exactly the
 * three things that identify the batch:
 *
 * <pre>{prefix}_{database}_{table}_{subtask}_{epoch}_{rung}</pre>
 *
 * <ul>
 *   <li>{@code epoch} is the id of the checkpoint the batch belongs to. The rows written after
 *       checkpoint E completed belong to E+1, which is why a restored writer starts at {@code
 *       restoredCheckpointId + 1} and every {@link #snapshotState} moves to {@code checkpointId +
 *       1}. Reusing an epoch already written would collide with a label Doris still remembers — and
 *       since that checkpoint is by definition one this job <em>completed</em>, the collision would
 *       be with a FINISHED load, which is not recoverable.
 *   <li>{@code rung} counts the batches within one epoch for that table. A checkpoint's rows are
 *       not the only ones flushed: the buffer threshold and the periodic timer flush too, so
 *       several batches can arrive under one epoch and each needs its own label.
 * </ul>
 *
 * <p>A label that is already in Doris's hands is not a dead end, which is what makes recovery work
 * without querying Doris: a restart hits the labels of the attempt that died, and those
 * transactions are still open. Doris's reply names the transaction holding the label, so this
 * writer aborts it and re-loads the same batch under the same label — doris-2.1.8 was measured to
 * free a label once its transaction is aborted. A label whose transaction has already FINISHED is a
 * different story and is reported as a failure instead: this job's label space has been used by
 * something else (another job sharing the prefix, or a savepoint older than the labels), and no
 * amount of retrying will fix it.
 */
public class TwoPhaseDorisSinkWriter extends StatefulDorisSinkWriter
        implements PrecommittingSinkWriter<Event, DorisCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseDorisSinkWriter.class);

    /**
     * How many times a label collision is cleared before giving up. One is enough in every case
     * seen: the colliding transaction is aborted and the label is then free. The bound is here so a
     * Doris that keeps reporting a collision cannot spin this loop forever.
     */
    private static final int LABEL_CONFLICT_ATTEMPTS = 3;

    private final int subtaskId;

    /**
     * The checkpoint the rows being written now belong to; see the class javadoc. It is part of
     * every label, and therefore of what a restarted job looks for.
     */
    private long currentEpoch;

    /** Batches already pre-committed per table in this epoch, i.e. the next free rung. */
    private final Map<String, Integer> nextRung = new HashMap<>();

    /**
     * The open transactions of the current epoch, by label. They are what {@link #prepareCommit()}
     * hands to the committer, and the set is emptied when the epoch turns over — the committer owns
     * them from then on, having received them under the checkpoint that just ended.
     */
    private final Map<String, DorisCommittable> openTransactions = new LinkedHashMap<>();

    public TwoPhaseDorisSinkWriter(
            DorisDataSinkOptions options, ZoneId pipelineZoneId, Sink.InitContext initContext) {
        super(options, pipelineZoneId, initContext);
        this.subtaskId = initContext.getSubtaskId();
        // A fresh job writes the rows that the first checkpoint will contain, so it starts at
        // INITIAL_CHECKPOINT_ID; a restored one starts at the checkpoint after the one it restored
        // from. Either way the epoch names the checkpoint the batch belongs to.
        OptionalLong restoredCheckpointId = initContext.getRestoredCheckpointId();
        this.currentEpoch =
                restoredCheckpointId.isPresent()
                        ? restoredCheckpointId.getAsLong() + 1
                        : Sink.InitContext.INITIAL_CHECKPOINT_ID;
    }

    @Override
    protected void landBatch(String database, String table, List<Map<String, Object>> rows)
            throws IOException {
        String key = database + "." + table;
        int rung = nextRung.getOrDefault(key, 0);
        String label = labelFor(database, table, rung);
        DorisHttpClient.PrecommitResult precommit = precommit(database, table, label, rows);
        // Only past a successful pre-commit: a batch that failed is retried by the task, and it has
        // to be retried under the label whose transaction it opens.
        nextRung.put(key, rung + 1);
        openTransactions.put(
                label, new DorisCommittable(database, table, label, precommit.getTransactionId()));
    }

    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        // The operator flushes before it calls this, so the set is complete for the checkpoint, and
        // the snapshot that follows records the rows it covers. The set is not cleared here: the
        // committables of this checkpoint are handed over once, and the epoch turns over in
        // snapshotState.
        return new ArrayList<>(openTransactions.values());
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        List<DorisWriterState> state = super.snapshotState(checkpointId);
        currentEpoch = checkpointId + 1;
        nextRung.clear();
        openTransactions.clear();
        return state;
    }

    @Override
    protected boolean shouldFlushOnClose() {
        return false;
    }

    /**
     * Pre-commits one batch under {@code label}, first clearing the label if a previous attempt
     * left a transaction on it.
     */
    private DorisHttpClient.PrecommitResult precommit(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        try {
            return precommitClearingLabelConflicts(database, table, label, rows);
        } catch (IOException e) {
            metrics.recordStreamLoadFailure();
            throw e;
        }
    }

    private DorisHttpClient.PrecommitResult precommitClearingLabelConflicts(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                DorisHttpClient.PrecommitResult precommit =
                        httpClient.precommitStreamLoad(database, table, label, rows);
                metrics.recordStreamLoad(precommit.getBodyBytes());
                return precommit;
            } catch (DorisHttpClient.LabelAlreadyExistsException e) {
                clearLabelConflict(database, table, label, e, attempt);
            }
        }
    }

    /**
     * Resolves a label collision, or fails when it cannot be resolved.
     *
     * <p>The recoverable case is a transaction that is still open: an earlier attempt of this job
     * (or of one restored from the same checkpoint) pre-committed this batch and died before the
     * batch was checkpointed. Aborting that transaction discards rows nobody will ever commit, and
     * frees the label for the batch about to be re-loaded.
     */
    private void clearLabelConflict(
            String database,
            String table,
            String label,
            DorisHttpClient.LabelAlreadyExistsException conflict,
            int attempt)
            throws IOException {
        if (conflict.isFinished()) {
            throw new IOException(
                    "Label '"
                            + label
                            + "' of "
                            + database
                            + "."
                            + table
                            + " was already used by a FINISHED load. The label prefix '"
                            + labelPrefix
                            + "' is in use by another job, or this job is restoring from a"
                            + " savepoint older than the labels it is now writing; neither can be"
                            + " resolved by retrying. Change "
                            + DorisDataSinkOptions.LABEL_PREFIX.key()
                            + " for this job.",
                    conflict);
        }
        if (!conflict.isPrecommitted() || conflict.getExistingTransactionId() < 0) {
            throw new IOException(
                    "Label '"
                            + label
                            + "' of "
                            + database
                            + "."
                            + table
                            + " is held by a transaction this job cannot clear (status "
                            + conflict.getExistingJobStatus()
                            + "). Doris must free the label before the batch can be loaded.",
                    conflict);
        }
        if (attempt >= LABEL_CONFLICT_ATTEMPTS) {
            throw new IOException(
                    "Label '"
                            + label
                            + "' of "
                            + database
                            + "."
                            + table
                            + " is still held after "
                            + attempt
                            + " attempts to clear it.",
                    conflict);
        }
        LOG.warn(
                "Label {} of {}.{} belongs to open transaction {}; aborting it and loading the batch"
                        + " under the same label again.",
                label,
                database,
                table,
                conflict.getExistingTransactionId());
        httpClient.abortTransaction(database, conflict.getExistingTransactionId());
    }

    private String labelFor(String database, String table, int rung) {
        return labelPrefix
                + "_"
                + sanitize(database)
                + "_"
                + sanitize(table)
                + "_"
                + subtaskId
                + "_"
                + currentEpoch
                + "_"
                + rung;
    }
}
