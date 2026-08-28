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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * The stream fetch task of the Kafka-json source: consumes the change-log messages written by a
 * canal / Debezium / TiCDC connector to Kafka and converts them into Debezium-shaped {@link
 * SourceRecord}s enqueued into the shared {@link io.debezium.connector.base.ChangeEventQueue}.
 *
 * <p>All partitions of the configured topics are assigned manually and seeked to the first message
 * whose event time is at or after the split's starting offset — the same lower bound the snapshot
 * phase used, so no change is skipped at the switch. A bounded read drops messages strictly after
 * its ending offset (they belong to the following stream phase) and emits the {@link
 * WatermarkKind#END} watermark once every partition has passed the ending offset; messages strictly
 * before the starting offset are dropped too, since the JDBC snapshot already captured them. The
 * unbounded stream split drops messages at or before its starting offset and, in a multi-split
 * stream, messages that its owning snapshot split's bounded backfill already emitted.
 *
 * <p>The consumed position is kept as {@link KafkaJsonOffset} and restored from Flink checkpoint
 * state; the Kafka group offset is committed only for external observability.
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

    /** The next Kafka offset to consume per partition, tracked as records are polled. */
    private final Map<TopicPartition, Long> consumedOffsets = new ConcurrentHashMap<>();

    /**
     * Offsets to commit to the Kafka consumer group, snapshotted on checkpoint completion and
     * committed by the fetcher thread after the next poll ({@link KafkaConsumer} is not
     * thread-safe). May be {@code null} when no commit is pending.
     */
    private volatile Map<TopicPartition, Long> pendingCommitOffsets;

    /** Partitions that have already delivered a message strictly after the ending offset. */
    private final Set<TopicPartition> partitionsPastEnding = new HashSet<>();
    /** The partitions the consumer was assigned to (populated by {@link #assignAndSeek}). */
    private volatile List<TopicPartition> assignedPartitions = new ArrayList<>();

    /** Applies the DDL messages to the shared schema; built once per execute. */
    private volatile KafkaJsonSchemaChangeHandler schemaChangeHandler;

    /** Parses the Kafka messages into the message abstraction; selected once per message format. */
    private volatile KafkaJsonMessageParser messageParser;

    /**
     * The stream split's finished snapshot splits, indexed by table. Empty for backfill splits,
     * whose splitId is the snapshot split id rather than {@link StreamSplit#STREAM_SPLIT_ID}.
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
                    commitPendingOffsets(consumer);
                    Thread.sleep(EMPTY_POLL_SLEEP_MILLIS);
                    continue;
                }
                if (processRecords(sourceFetchContext, records) && isBoundedRead()) {
                    // the last message of this batch reached the ending offset
                    dispatchEndWatermark(sourceFetchContext);
                    break;
                }
                commitPendingOffsets(consumer);
            }
        } finally {
            taskRunning = false;
        }
    }

    /**
     * Consumes one poll batch: parses each message, converts it into the data records and enqueues
     * them. Returns {@code true} when the bounded read is over — that is, every assigned partition
     * has delivered a message strictly after the ending offset.
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
            lastOffset = toOffset(message, record, sourceFetchContext);
            // track the next offset to consume per partition (for the checkpoint-time group commit)
            consumedOffsets.put(
                    new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);

            if (shouldDropAsStreamPhaseResidual(lastOffset)) {
                continue;
            }

            if (shouldSkipBelowLowWatermark(lastOffset)) {
                continue;
            }

            if (shouldSkipPastEndingOffset(record, lastOffset)) {
                continue;
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
                if (shouldDropAsMultiSplitResidual(sourceFetchContext, sourceRecord, lastOffset)) {
                    continue;
                }
                sourceFetchContext.getQueue().enqueue(new DataChangeEvent(sourceRecord));
            }
        }
        return isBoundedReadComplete(lastOffset);
    }

    private KafkaJsonOffset toOffset(
            KafkaJsonMessage message,
            ConsumerRecord<String, String> record,
            KafkaJsonSourceFetchTaskContext context) {
        return new KafkaJsonOffset(
                KafkaJsonRecordConverter.eventTime(
                        message, context.getSourceConfig().getEventTime()),
                record.partition(),
                record.offset());
    }

    /**
     * Drops messages at or before the stream starting offset (the minimum high watermark over the
     * finished snapshot splits): they were already emitted by the bounded backfill or are reflected
     * in the JDBC snapshot, so re-emitting them on the stream path would duplicate the incremental
     * snapshot. The bounded backfill split (splitId is the snapshot split id, not {@link
     * StreamSplit#STREAM_SPLIT_ID}) keeps its inclusive bounds and is untouched.
     */
    boolean shouldDropAsStreamPhaseResidual(KafkaJsonOffset offset) {
        return StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
                && offset.isAtOrBefore(startingOffset);
    }

    /**
     * Drops a message below the low watermark: a pre-snapshot change whose effect the JDBC snapshot
     * already captured (possibly superseded by a later change), so replaying it would override the
     * snapshot row with a stale value.
     */
    boolean shouldSkipBelowLowWatermark(KafkaJsonOffset offset) {
        return isBoundedRead() && offset.getEventTime() < startingOffset.getEventTime();
    }

    /**
     * Skips a message strictly after the ending offset — it belongs to the following stream phase
     * and would otherwise be emitted twice — and records that the partition has crossed the ending
     * offset so {@link #isBoundedReadComplete} can finish the bounded read.
     */
    boolean shouldSkipPastEndingOffset(
            ConsumerRecord<String, String> record, KafkaJsonOffset offset) {
        if (!isBoundedRead() || !offset.isAfter(endingOffset)) {
            return false;
        }
        partitionsPastEnding.add(new TopicPartition(record.topic(), record.partition()));
        return true;
    }

    /**
     * Drops the multi-split residual: a message already emitted by the bounded backfill of the
     * finished snapshot split that owns its primary key (event time strictly after the stream
     * starting offset but at or before that split's high watermark). A genuinely new change after
     * the owning high watermark still passes through.
     */
    boolean shouldDropAsMultiSplitResidual(
            KafkaJsonSourceFetchTaskContext context,
            SourceRecord sourceRecord,
            KafkaJsonOffset offset) {
        return StreamSplit.STREAM_SPLIT_ID.equals(split.splitId())
                && isCoveredByFinishedSnapshotSplit(context, sourceRecord, offset);
    }

    /**
     * Returns whether the bounded read is complete, i.e. every assigned partition has crossed the
     * ending offset. Terminating on the first crossing record would silently drop the unread
     * messages of a lagging partition, so the poll loop keeps going until they arrive.
     */
    private boolean isBoundedReadComplete(KafkaJsonOffset lastOffset) {
        if (lastOffset != null) {
            currentOffset = lastOffset;
        }
        if (!isBoundedRead()) {
            return false;
        }
        for (TopicPartition partition : assignedPartitions) {
            if (!partitionsPastEnding.contains(partition)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether {@code sourceRecord} — carrying the message event time {@code lastOffset} —
     * was already covered by the bounded backfill of one of the finished snapshot splits of its
     * table: its primary key falls in the split's range and its event time is at-or-before the
     * split's high watermark. Such a record must not be re-emitted by the stream phase.
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

    /**
     * Records the latest checkpointed position and snapshots the consumed Kafka offsets for the
     * next group commit. Called from the checkpoint-complete callback on the task thread; the
     * actual Kafka commit runs on the fetcher thread ({@link KafkaConsumer} is not thread-safe).
     * The group offset is never read back — exactly-once recovery relies on Flink checkpoint state.
     */
    public void commitCurrentOffset(Offset offset) {
        if (offset instanceof KafkaJsonOffset) {
            KafkaJsonOffset kafkaJsonOffset = (KafkaJsonOffset) offset;
            if (lastCommittedOffset == null || kafkaJsonOffset.isAfter(lastCommittedOffset)) {
                lastCommittedOffset = kafkaJsonOffset;
            }
            LOG.debug("Committing offset: {}", lastCommittedOffset);
            pendingCommitOffsets = new HashMap<>(consumedOffsets);
        }
    }

    /**
     * Commits the offsets pending from the last checkpoint completion to the Kafka consumer group.
     * Must run on the fetcher thread (the only thread allowed to touch the {@link KafkaConsumer});
     * the committed offsets are informational only, since the connector never resumes from them.
     */
    private void commitPendingOffsets(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, Long> pending = pendingCommitOffsets;
        if (pending == null || pending.isEmpty()) {
            return;
        }
        pendingCommitOffsets = null;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : pending.entrySet()) {
            offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        try {
            consumer.commitSync(offsets);
            LOG.debug("Committed Kafka group offsets: {}", offsets);
        } catch (Exception e) {
            // informational only; a failed commit must not fail the task
            LOG.warn("Failed to commit Kafka group offsets", e);
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
