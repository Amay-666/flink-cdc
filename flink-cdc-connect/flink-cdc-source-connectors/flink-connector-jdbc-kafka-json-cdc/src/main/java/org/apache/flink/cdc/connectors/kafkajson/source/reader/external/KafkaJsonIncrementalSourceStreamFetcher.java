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

package org.apache.flink.cdc.connectors.kafkajson.source.reader.external;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.Fetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Fetcher to fetch data from table split, the split is the stream split {@link StreamSplit}. */
public class KafkaJsonIncrementalSourceStreamFetcher
        implements Fetcher<SourceRecords, SourceSplitBase> {
    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaJsonIncrementalSourceStreamFetcher.class);

    private final FetchTask.Context taskContext;
    private final ExecutorService executorService;
    private final Set<TableId> pureStreamPhaseTables;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;
    private StreamSplit currentStreamSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, Offset> maxSplitHighWatermarkMap;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public KafkaJsonIncrementalSourceStreamFetcher(FetchTask.Context taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureStreamPhaseTables = new HashSet<>();
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.streamFetchTask = fetchTask;
        this.currentStreamSplit = fetchTask.getSplit().asStreamSplit();
        configureFilter();
        taskContext.configure(currentStreamSplit);
        this.queue = taskContext.getQueue();
        executorService.submit(
                () -> {
                    try {
                        streamFetchTask.execute(taskContext);
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute stream read task for stream split %s fail",
                                        currentStreamSplit),
                                e);
                        readException = e;
                    } finally {
                        try {
                            stopReadTask();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentStreamSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                } else {
                    LOG.debug("{} data change event should not emit", event);
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentStreamSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            // gracefully stop streamFetchTask, e.g. during shutdown
            stopReadTask();
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(
                        READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the stream fetcher in {} seconds.",
                            READER_CLOSE_TIMEOUT_SECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("Close stream fetcher error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the stream split reader only sends the change event that
     * belongs to its finished snapshot splits. For each snapshot split, the change event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the change event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the change event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     *
     * <p>Three cases the watermark rule cannot decide are settled against the schema store, the
     * registry of the tables this source knows:
     *
     * <ul>
     *   <li>a table whose snapshot split infos never covered it inside this split — a table renamed
     *       after its snapshot (the infos and watermarks keep the pre-rename name, and no record
     *       can ever match them again) or a table created while the job runs. It is emitted once
     *       its schema is in the store. The rule is deliberately the store rather than "the table
     *       is in this split's {@code tableSchemas}": the store is what a restarted job rebuilds
     *       from the checkpointed split state, so the same decision is reached again after a
     *       failure;
     *   <li>a table the store no longer knows, whose records were nevertheless converted before the
     *       DDL that removed or renamed it (the fetch task applies a DDL as it consumes it, while
     *       the reader drains the queue behind it). Such a record is emitted: the queue delivers it
     *       ahead of the DDL that changed the table, so the sink still has the table when it
     *       arrives, and the watermark rule cannot judge it at all — it would need the table's
     *       split column, which is exactly what is gone;
     *   <li>a non-data-change record (a schema change or a signal event), which is always emitted
     *       so that it reaches the state of Flink.
     * </ul>
     *
     * <p>The widened scope of the first case is deliberate: a record in between is delivered once
     * by this path and once more by the backfill, so a sink must tolerate a repeated change of the
     * same row (the Doris sink upserts by primary key). Withholding them instead would delay them,
     * not drop them — the backfill covers them — which is why the released logic did so, and which
     * is what made a renamed table's records disappear: nothing covers them any more.
     */
    @VisibleForTesting
    boolean shouldEmit(SourceRecord sourceRecord) {
        if (!taskContext.isDataChangeRecord(sourceRecord)) {
            // always send the schema change event and signal event
            // we need record them to state of Flink
            return true;
        }
        TableId tableId = taskContext.getTableId(sourceRecord);
        Offset position = taskContext.getStreamOffset(sourceRecord);
        if (hasEnterPureStreamPhase(tableId, position)) {
            return true;
        }
        RelationalDatabaseSchema schema = schemaStore();
        if (schema != null && schema.tableFor(tableId) == null) {
            LOG.debug(
                    "Table {} is no longer in the schema store — dropped, or renamed to another "
                            + "name — but the record precedes that DDL in the stream; emitting it",
                    tableId);
            return true;
        }
        // only the table who captured snapshot splits need to filter
        if (finishedSplitsInfo.containsKey(tableId)) {
            for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                if (taskContext.isRecordBetween(
                                sourceRecord, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                        && position.isAfter(splitInfo.getHighWatermark())) {
                    return true;
                }
            }
            // No split of this table admits the record: it lies inside a snapshot split whose
            // backfill has already emitted it, and is dropped as a duplicate.
            return false;
        }
        pureStreamPhaseTables.add(tableId);
        LOG.info(
                "Table {} is not covered by any finished snapshot split of this split, but is "
                        + "present in the schema store; emitting its changes",
                tableId);
        return true;
    }

    /**
     * Returns the schema store of the fetch task context, or {@code null} when the context is not a
     * JDBC source context (the released code's way of staying usable with any {@link FetchTask}
     * context; every context this fetcher is built with is a {@link JdbcSourceFetchTaskContext}).
     */
    @Nullable
    private RelationalDatabaseSchema schemaStore() {
        return taskContext instanceof JdbcSourceFetchTaskContext
                ? ((JdbcSourceFetchTaskContext) taskContext).getDatabaseSchema()
                : null;
    }

    private boolean hasEnterPureStreamPhase(TableId tableId, Offset position) {
        if (pureStreamPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureStreamPhaseTables.add(tableId);
            return true;
        }

        // Use still need to capture new sharding table if user disable scan new added table,
        // The history records for all new added tables(including sharding table and normal table)
        // will be capture after restore from a savepoint if user enable scan new added table
        if (!taskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // the new added sharding table without history records
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && taskContext.getTableFilter().isIncluded(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentStreamSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, Offset> tableIdOffsetPositionMap = new HashMap<>();
        // startup mode which is stream only
        if (taskContext.getSourceConfig().getStartupOptions().isStreamOnly()) {
            for (TableId tableId : currentStreamSplit.getTableSchemas().keySet()) {
                tableIdOffsetPositionMap.put(tableId, currentStreamSplit.getStartingOffset());
            }
        }
        // startup mode which includes snapshot phase
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                Offset highWatermark = finishedSplitInfo.getHighWatermark();
                Offset maxHighWatermark = tableIdOffsetPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdOffsetPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdOffsetPositionMap;
        this.pureStreamPhaseTables.clear();
    }

    public void stopReadTask() throws Exception {
        this.currentTaskRunning = false;

        if (taskContext != null) {
            taskContext.close();
        }

        if (streamFetchTask != null) {
            streamFetchTask.close();
        }
    }
}
