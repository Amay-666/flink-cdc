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

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.kafka.KafkaJsonKafkaOffsetUtils;
import org.apache.flink.cdc.connectors.kafkajson.source.kafka.KafkaJsonOffsetSupplier;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage.MessageType;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonParserFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordConverter;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The stream fetch task of the Kafka-json source: consumes the change-log messages written by canal
 * (or a Debezium / TiCDC connector) to Kafka and converts them into the Debezium-shaped {@link
 * SourceRecord}s that are enqueued into the shared {@link io.debezium.connector.base.ChangeEventQueue}.
 *
 * <p>The reader consumes from all partitions of the configured topics (manually assigned, not group
 * managed). Each partition is seeked to the first message whose event time (canal {@code es}/{@code
 * ts}, or Debezium {@code source.ts_ms}/{@code payload.ts_ms} for the configured {@code EventTime}
 * mode) is at or after the stream split's starting offset — the same conservative lower bound that
 * {@link KafkaJsonOffsetSupplier} computed during the snapshot phase, so no change is skipped at
 * the full-&gt;incremental switch.
 *
 * <p>When the split is a bounded read (its ending offset is a real event time rather than {@link
 * KafkaJsonOffset#NO_STOPPING_OFFSET}) the task dispatches the {@link WatermarkKind#END} watermark
 * once every partition has delivered a message at-or-after the ending offset (or is drained to its
 * end), which finalizes the incremental snapshot of the split. Messages <em>strictly after</em> the
 * ending offset are not emitted: the stream split that follows reads only event times strictly
 * after its starting offset (its high-watermark watermark), so emitting them here would duplicate
 * the stream-phase emission. The ending offset itself is <em>not</em> after the ending offset (the
 * watermark carries the sentinel partition/offset, see {@link KafkaJsonKafkaOffsetUtils}) and is
 * therefore backfilled exactly once. Messages <em>strictly before</em> the starting offset — the
 * split's low watermark — are pre-snapshot changes whose effect the JDBC read has already captured
 * (possibly superseded by a later change), and are dropped so they cannot override the snapshot
 * with a stale value.
 *
 * <p>In the unbounded stream phase (the {@code stream-split}) the task enforces the strict
 * exclusive lower bound: every message at-or-before the starting offset — the minimum high
 * watermark over the finished snapshot splits — is dropped, because the bounded backfill of its
 * owning snapshot split has already emitted it or the JDBC snapshot already reflects it. A message
 * strictly after that lower bound but still at-or-before the high watermark of the finished
 * snapshot split that owns its primary key is dropped as well (the multi-split residual). Both
 * guards keep the stream phase from re-emitting a change the incremental snapshot has already
 * delivered once the base pure-stream filter is bypassed; see docs/BOUNDARY_AUDIT.md.
 *
 * <p>The position of the most recent consumed message is kept as {@link KafkaJsonOffset} and
 * committed by {@link KafkaJsonDialect#notifyCheckpointComplete(long, Offset)}; the Kafka group
 * offset itself is left untouched because exactly-once recovery relies on the Flink checkpointed
 * state.
 */
public class KafkaJsonStreamFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonStreamFetchTask.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500L);
    /** Sleep between empty polls so that a mock/empty consumer does not busy-spin. */
    private static final long EMPTY_POLL_SLEEP_MILLIS = 10L;

    private final StreamSplit split;
    private final KafkaJsonOffset startingOffset;
    private final KafkaJsonOffset endingOffset;

    private volatile boolean taskRunning = false;
    private volatile boolean stopped = false;

    /** The Kafka consumer, owned by the {@link KafkaJsonSourceFetchTaskContext}. */
    private volatile KafkaConsumer<String, String> consumer;

    /** The position of the last consumed message. */
    private volatile KafkaJsonOffset currentOffset;

    private volatile KafkaJsonOffset lastCommittedOffset;

    /**
     * Partitions that have already delivered a message at-or-after the ending offset (bounded
     * reads).
     */
    private final Set<TopicPartition> partitionsPastEnding = new HashSet<>();
    /** The partitions the consumer was assigned to (populated by {@link #assignAndSeek}). */
    private volatile List<TopicPartition> assignedPartitions = new ArrayList<>();
    /**
     * Partition log ends captured when the bounded read starts; a drained partition counts as done.
     */
    private volatile Map<TopicPartition, Long> partitionEndOffsets = new HashMap<>();

    /** Applies the DDL messages to the shared schema; built once per execute. */
    private volatile KafkaJsonSchemaChangeHandler schemaChangeHandler;

    /** Parses the Kafka messages into the message abstraction; selected once per message format. */
    private volatile KafkaJsonMessageParser messageParser;

    /**
     * The finished snapshot splits of the stream split, indexed by table. Each split carries the
     * primary-key range and the high watermark of the bounded backfill that already replayed it, so
     * the stream phase can drop in-range records whose event time is still at-or-before that high
     * watermark (the {@code finishedSnapshotSplitInfos} of the stream split). Empty for backfill
     * splits, whose splitId is the snapshot split id rather than {@link
     * StreamSplit#STREAM_SPLIT_ID}.
     */
    private final Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsByTable =
            new HashMap<>();

    public KafkaJsonStreamFetchTask(StreamSplit split) {
        this.split = split;
        this.startingOffset =
                split.getStartingOffset() == null
                        ? KafkaJsonOffset.INITIAL_OFFSET
                        : (KafkaJsonOffset) split.getStartingOffset();
        this.endingOffset =
                split.getEndingOffset() == null
                        ? KafkaJsonOffset.NO_STOPPING_OFFSET
                        : (KafkaJsonOffset) split.getEndingOffset();
        this.currentOffset = startingOffset;
        for (FinishedSnapshotSplitInfo finishedSplit : split.getFinishedSnapshotSplitInfos()) {
            finishedSplitsByTable
                    .computeIfAbsent(finishedSplit.getTableId(), ignored -> new ArrayList<>())
                    .add(finishedSplit);
        }
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        if (stopped) {
            LOG.debug(
                    "StreamFetchTask for split: {} is already stopped and can not be executed",
                    split);
            return;
        }
        LOG.debug("Execute StreamFetchTask for split: {}", split);
        if (!(context instanceof KafkaJsonSourceFetchTaskContext)) {
            throw new FlinkRuntimeException(
                    "Unexpected FetchTask.Context type: " + context.getClass().getName());
        }
        KafkaJsonSourceFetchTaskContext sourceFetchContext =
                (KafkaJsonSourceFetchTaskContext) context;
        KafkaJsonSourceConfig sourceConfig = sourceFetchContext.getSourceConfig();
        this.schemaChangeHandler = new KafkaJsonSchemaChangeHandler(sourceConfig);
        this.messageParser = KafkaJsonParserFactory.create(sourceConfig.getMessageFormat());
        taskRunning = true;
        try {
            this.consumer = sourceFetchContext.getKafkaConsumer();
            assignAndSeek(consumer, sourceConfig);
            if (isBoundedRead()) {
                // capture the partition log ends once the consumer is positioned: the bounded read
                // is
                // over when every partition has delivered a message at-or-after the ending offset
                // or
                // has been drained to its end (e.g. an empty partition, which has nothing to read)
                this.partitionEndOffsets = consumer.endOffsets(this.assignedPartitions);
            }
            while (taskRunning && !stopped) {
                ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(POLL_TIMEOUT);
                } catch (WakeupException e) {
                    // close() was called from another thread to interrupt the blocking poll
                    break;
                }
                if (stopped) {
                    break;
                }
                if (records.isEmpty()) {
                    // avoid a busy loop when there is no message (or the consumer is a mock)
                    Thread.sleep(EMPTY_POLL_SLEEP_MILLIS);
                    continue;
                }
                if (processRecords(sourceFetchContext, records) && isBoundedRead()) {
                    // the last message of this batch reached the ending offset
                    dispatchEndWatermark(sourceFetchContext);
                    break;
                }
            }
        } finally {
            taskRunning = false;
        }
    }

    /**
     * Consumes one poll batch: parses each message, converts it into the data records and enqueues
     * them. Returns {@code true} when the bounded read is over — that is, every assigned partition
     * has delivered a message at-or-after the ending offset, or has been drained to its end.
     */
    private boolean processRecords(
            KafkaJsonSourceFetchTaskContext sourceFetchContext,
            ConsumerRecords<String, String> records)
            throws Exception {
        KafkaJsonOffset lastOffset = null;
        for (ConsumerRecord<String, String> record : records) {
            if (stopped) {
                break;
            }
            KafkaJsonMessage message = messageParser.parse(record.value());
            if (message == null) {
                LOG.warn(
                        "Ignoring unparsable {} message at {}-{}@{}",
                        sourceFetchContext.getSourceConfig().getMessageFormat(),
                        record.topic(),
                        record.partition(),
                        record.offset());
                continue;
            }
            if (sourceFetchContext.getSourceConfig().getDatabaseType()
                            == KafkaJsonSourceOptions.DatabaseType.TIDB
                    && sourceFetchContext.getSourceConfig().getEventTime()
                            == KafkaJsonSourceOptions.EventTime.TIDB_TSO
                    && message.getMessageType() == KafkaJsonMessage.MessageType.TIDB_WATERMARK) {
                // A TiDB watermark message carries no TSO; in TIDB_TSO mode it would pollute the
                // offset bookkeeping with a meaningless time, so skip it before any processing.
                continue;
            }
            lastOffset =
                    new KafkaJsonOffset(
                            KafkaJsonRecordConverter.eventTime(
                                    message, sourceFetchContext.getSourceConfig().getEventTime()),
                            record.partition(),
                            record.offset());

            if (StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
                    && lastOffset.isAtOrBefore(startingOffset)) {
                // The stream split reads strictly after its starting offset (the minimum high
                // watermark over the finished snapshot splits). Once the base
                // IncrementalSourceStreamFetcher#hasEnterPureStreamPhase fires — the first record
                // at
                // or after a table's max high watermark — the per-split shouldEmit filter is
                // short-circuited, so an already-backfilled message (es <= its owning split's high
                // watermark) that is read after that trigger would otherwise be re-emitted here on
                // top of the bounded backfill (the F4 double, see docs/BOUNDARY_AUDIT.md). Every
                // message at-or-before the starting offset is covered by the bounded backfill
                // ([LOW, HIGH], inclusive) or by the JDBC snapshot (es < LOW), so it is dropped,
                // not
                // emitted, on the stream path. The backfill split (whose splitId is the snapshot
                // split id, not STREAM_SPLIT_ID) keeps its inclusive bounds and is untouched here.
                continue;
            }

            if (isBoundedRead()) {
                if (lastOffset.getEventTime() < startingOffset.getEventTime()) {
                    // Below the low watermark: a pre-snapshot change whose effect the JDBC read has
                    // already captured (the snapshot may even reflect a later change that
                    // supersedes
                    // it). Replaying it here could override the snapshot row with a stale value, so
                    // it is dropped; the low watermark is the inclusive lower bound of the
                    // backfill.
                    continue;
                }
                if (lastOffset.isAfter(endingOffset)) {
                    // A message strictly after the ending offset belongs to the stream phase, which
                    // reads only event times strictly after its starting offset, so emitting it
                    // here
                    // would duplicate the stream-phase emission. Drop it and remember that this
                    // partition has crossed the ending offset. The boundary message (event time
                    // equal
                    // to the ending offset; the watermark carries the sentinel partition/offset, so
                    // isAfter is false) is backfilled exactly once by this bounded read.
                    partitionsPastEnding.add(
                            new TopicPartition(record.topic(), record.partition()));
                    continue;
                }
            }

            if (message.getMessageType() == MessageType.DDL) {
                // DDL: apply the schema change to the shared schema and (when configured) emit the
                // schema-change record; no data record is produced
                handleDdlMessage(sourceFetchContext, message, lastOffset);
                continue;
            }
            List<SourceRecord> sourceRecords =
                    sourceFetchContext
                            .getRecordConverter()
                            .convert(message, record.topic(), record.partition(), record.offset());
            for (SourceRecord sourceRecord : sourceRecords) {
                if (StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
                        && isCoveredByFinishedSnapshotSplit(
                                sourceFetchContext, sourceRecord, lastOffset)) {
                    // The F4 residual in a multi-split stream: a message whose event time lies in
                    // (startingOffset, owning split's high watermark] — strictly after the stream
                    // starting offset but still at-or-before the high watermark of the finished
                    // snapshot split that owns its primary key — was already emitted by that
                    // split's
                    // bounded backfill. Once the pure-stream phase fires for the table, the base
                    // filter no longer drops it, so this pre-filter does, exactly as the base
                    // per-split shouldEmit would in single-partition order (see
                    // docs/BOUNDARY_AUDIT.md).
                    // A genuinely new change (es after the owning high watermark) still passes
                    // through.
                    continue;
                }
                sourceFetchContext.getQueue().enqueue(new DataChangeEvent(sourceRecord));
            }
        }
        if (lastOffset != null) {
            currentOffset = lastOffset;
        }
        if (!isBoundedRead()) {
            // unbounded stream read: never finishes on its own
            return false;
        }
        // Only when every partition has crossed the ending offset (or is drained to its end) is the
        // bounded read complete. Terminating on the first crossing record would silently drop the
        // still-unread messages of a lagging partition, so the loop keeps polling until they
        // arrive.
        for (TopicPartition partition : assignedPartitions) {
            if (!partitionsPastEnding.contains(partition)
                    && consumer.position(partition)
                            < partitionEndOffsets.getOrDefault(partition, 0L)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether {@code sourceRecord} — carrying the message event time {@code lastOffset} —
     * was already covered by the bounded backfill of one of the finished snapshot splits of its
     * table: its primary key falls in the split's range and its event time is at-or-before the
     * split's high watermark. Such a record must not be re-emitted by the stream phase once the
     * base pure-stream filter is bypassed (the F4 double).
     */
    private boolean isCoveredByFinishedSnapshotSplit(
            KafkaJsonSourceFetchTaskContext context,
            SourceRecord sourceRecord,
            KafkaJsonOffset lastOffset) {
        TableId tableId = context.getTableId(sourceRecord);
        if (context.getDatabaseSchema().tableFor(tableId) == null) {
            // the split key type cannot be resolved for a table the shared schema does not know
            // (yet); never drop a record we cannot classify
            return false;
        }
        for (FinishedSnapshotSplitInfo finishedSplit :
                finishedSplitsByTable.getOrDefault(tableId, Collections.emptyList())) {
            if (lastOffset.isAtOrBefore(finishedSplit.getHighWatermark())
                    && context.isRecordBetween(
                            sourceRecord,
                            finishedSplit.getSplitStart(),
                            finishedSplit.getSplitEnd())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Routes a DDL message to the schema-change pipeline (parser + shared-schema update + record).
     */
    private void handleDdlMessage(
            KafkaJsonSourceFetchTaskContext sourceFetchContext,
            KafkaJsonMessage message,
            KafkaJsonOffset offset)
            throws IOException, InterruptedException {
        KafkaJsonSchemaChangeHandler handler = schemaChangeHandler;
        if (handler == null) {
            LOG.warn("Schema-change handler not initialized; ignoring DDL: {}", message.getSql());
            return;
        }
        handler.handle(sourceFetchContext, message, offset);
    }

    /** Assigns all partitions of the configured topics and seeks each to the stream start. */
    private void assignAndSeek(
            KafkaConsumer<String, String> consumer, KafkaJsonSourceConfig sourceConfig) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : sourceConfig.getKafkaTopics()) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null) {
                throw new FlinkRuntimeException(
                        "Kafka topic '"
                                + topic
                                + "' does not exist on "
                                + sourceConfig.getKafkaBootstrapServers());
            }
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
        if (partitions.isEmpty()) {
            throw new FlinkRuntimeException(
                    "No Kafka partition found for topics: " + sourceConfig.getKafkaTopics());
        }
        this.assignedPartitions = partitions;
        consumer.assign(partitions);

        if (startingOffset.getEventTime() > 0) {
            // seek each partition to the first message whose event time >= the starting offset;
            // partitions without such a message (e.g. no change yet) start from the end
            Map<TopicPartition, Long> timestampMap = new HashMap<>();
            for (TopicPartition partition : partitions) {
                timestampMap.put(partition, startingOffset.getEventTime());
            }
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                    consumer.offsetsForTimes(timestampMap);
            for (TopicPartition partition : partitions) {
                OffsetAndTimestamp offsetAndTimestamp =
                        offsetsForTimes == null ? null : offsetsForTimes.get(partition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(partition, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToEnd(Collections.singleton(partition));
                }
            }
        } else {
            // no prior snapshot: start from the configured Kafka startup position
            switch (sourceConfig.getKafkaStartupMode()) {
                case LATEST:
                    consumer.seekToEnd(partitions);
                    break;
                case TIMESTAMP:
                    // fall through to earliest until a dedicated timestamp option is added
                case EARLIEST:
                default:
                    consumer.seekToBeginning(partitions);
                    break;
            }
        }
    }

    /** Dispatches the {@link WatermarkKind#END} watermark, finishing the incremental snapshot. */
    private void dispatchEndWatermark(KafkaJsonSourceFetchTaskContext sourceFetchContext)
            throws Exception {
        LOG.debug("StreamSplit is bounded read: {}", split);
        sourceFetchContext
                .getDispatcher()
                .dispatchWatermarkEvent(
                        sourceFetchContext.getPartition().getSourcePartition(),
                        split,
                        endingOffset,
                        WatermarkKind.END);
        LOG.info("StreamFetchTask finished for {} at {}", split, endingOffset);
    }

    private boolean isBoundedRead() {
        return endingOffset.getEventTime() != KafkaJsonOffset.NO_STOPPING_OFFSET.getEventTime();
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    @Override
    public void close() {
        LOG.debug("Stopping StreamFetchTask for split: {}", split);
        stopped = true;
        taskRunning = false;
        KafkaConsumer<String, String> consumer = this.consumer;
        if (consumer != null) {
            // interrupt the blocking poll; the consumer itself is closed by the task context
            consumer.wakeup();
        }
    }

    /** Commits the offset of the latest checkpoint to the stream position. */
    public void commitCurrentOffset(Offset offset) {
        if (offset instanceof KafkaJsonOffset) {
            KafkaJsonOffset canalOffset = (KafkaJsonOffset) offset;
            if (lastCommittedOffset == null || canalOffset.isAfter(lastCommittedOffset)) {
                lastCommittedOffset = canalOffset;
            }
            LOG.debug("Committing offset: {}", lastCommittedOffset);
            // The Kafka group offset is intentionally not committed: exactly-once recovery is
            // provided by the Flink checkpointed state (see the class javadoc).
        }
    }

    /** Returns the position of the last consumed message. */
    public KafkaJsonOffset getCurrentOffset() {
        return currentOffset;
    }

    /** Returns the offset of the last committed checkpoint. */
    public KafkaJsonOffset getLastCommittedOffset() {
        return lastCommittedOffset;
    }

    /** Returns the ending offset of this stream split. */
    public KafkaJsonOffset getEndingOffset() {
        return endingOffset;
    }
}
