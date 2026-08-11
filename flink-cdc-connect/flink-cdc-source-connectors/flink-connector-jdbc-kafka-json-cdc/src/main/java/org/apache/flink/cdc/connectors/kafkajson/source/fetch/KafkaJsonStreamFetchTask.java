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
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonFlatMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonFlatMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonRecordConverter;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.pipeline.DataChangeEvent;
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
import java.util.List;
import java.util.Map;

/**
 * The stream fetch task of the Canal source: consumes the change-log messages written by canal to
 * Kafka and converts them into the Debezium-shaped {@link SourceRecord}s that are enqueued into the
 * shared {@link io.debezium.connector.base.ChangeEventQueue}.
 *
 * <p>The reader consumes from all partitions of the configured topics (manually assigned, not group
 * managed). Each partition is seeked to the first message whose canal event time ({@code es}/{@code
 * ts}) is at or after the stream split's starting offset — the same conservative lower bound that
 * {@link KafkaJsonOffsetSupplier} computed during the snapshot phase, so no change is skipped at the
 * full-&gt;incremental switch.
 *
 * <p>When the split is a bounded read (its ending offset is a real event time rather than {@link
 * KafkaJsonOffset#NO_STOPPING_OFFSET}) the task dispatches the {@link WatermarkKind#END} watermark once
 * it has consumed past the ending offset, which finalizes the incremental snapshot of the split.
 *
 * <p>The position of the most recent consumed message is kept as {@link KafkaJsonOffset} and committed
 * by {@link KafkaJsonDialect#notifyCheckpointComplete(long, Offset)}; the Kafka group offset itself is
 * left untouched because exactly-once recovery relies on the Flink checkpointed state.
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

    /** Applies the DDL messages to the shared schema; built once per execute. */
    private volatile KafkaJsonSchemaChangeHandler schemaChangeHandler;

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
        KafkaJsonSourceFetchTaskContext sourceFetchContext = (KafkaJsonSourceFetchTaskContext) context;
        KafkaJsonSourceConfig sourceConfig = sourceFetchContext.getSourceConfig();
        this.schemaChangeHandler = new KafkaJsonSchemaChangeHandler(sourceConfig);
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
     * them. Returns {@code true} when the last consumed message reached the ending offset.
     */
    private boolean processRecords(
            KafkaJsonSourceFetchTaskContext sourceFetchContext, ConsumerRecords<String, String> records)
            throws Exception {
        KafkaJsonOffset lastOffset = null;
        for (ConsumerRecord<String, String> record : records) {
            if (stopped) {
                break;
            }
            KafkaJsonFlatMessage message = KafkaJsonFlatMessageParser.parse(record.value());
            if (message == null) {
                LOG.warn(
                        "Ignoring unparsable canal message at {}-{}@{}",
                        record.topic(),
                        record.partition(),
                        record.offset());
                continue;
            }
            lastOffset =
                    new KafkaJsonOffset(
                            KafkaJsonRecordConverter.eventTime(
                                    message, sourceFetchContext.getSourceConfig().getEventTime()),
                            record.partition(),
                            record.offset());

            if (message.isDdl()) {
                // DDL: apply the schema change to the shared schema and (when configured) emit the
                // schema-change record; no data record is produced
                handleDdlMessage(sourceFetchContext, message, lastOffset);
                continue;
            }
            List<SourceRecord> sourceRecords =
                    sourceFetchContext
                            .getRecordConverter()
                            .convert(
                                    message,
                                    record.topic(),
                                    record.partition(),
                                    record.offset());
            for (SourceRecord sourceRecord : sourceRecords) {
                sourceFetchContext.getQueue().enqueue(new DataChangeEvent(sourceRecord));
            }
        }
        if (lastOffset != null) {
            currentOffset = lastOffset;
        }
        return currentOffset.getEventTime() >= endingOffset.getEventTime();
    }

    /** Routes a DDL message to the schema-change pipeline (parser + shared-schema update + record). */
    private void handleDdlMessage(
            KafkaJsonSourceFetchTaskContext sourceFetchContext,
            KafkaJsonFlatMessage message,
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
    private void assignAndSeek(KafkaConsumer<String, String> consumer, KafkaJsonSourceConfig sourceConfig) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : sourceConfig.getKafkaTopics()) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null) {
                throw new FlinkRuntimeException(
                        "Kafka topic '" + topic + "' does not exist on "
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
