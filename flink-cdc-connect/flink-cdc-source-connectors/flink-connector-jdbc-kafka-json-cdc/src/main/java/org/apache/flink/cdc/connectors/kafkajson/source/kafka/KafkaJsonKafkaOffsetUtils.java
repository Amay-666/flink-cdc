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

package org.apache.flink.cdc.connectors.kafkajson.source.kafka;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.cdc.connectors.kafkajson.source.message.DebeziumMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.DebeziumMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonKafkaUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities to query the current change-log position from Kafka.
 *
 * <p>Unlike a binlog position, there is no single "current offset" of the change-log stream: each
 * Kafka partition has its own position. We therefore define the current position as the <b>maximum
 * message event time across all partitions' newest messages</b>, stamped onto a sentinel
 * partition/offset ({@link Integer#MAX_VALUE}, {@link Long#MAX_VALUE}). Because a real message's
 * partition is always smaller than the sentinel, a message whose event time equals the watermark is
 * ordered <em>before</em> it: the bounded backfill of a snapshot split owns the boundary message
 * (inclusive at the high watermark) and the stream phase emits only event times strictly after the
 * watermark. A change committed while the snapshot split's JDBC read is running therefore has its
 * event time inside the split's {@code (low, high]} backfill window, is replayed exactly once by
 * the backfill, and is never re-emitted by the stream — the full-&gt;incremental switch is
 * exactly-once for those changes. (A minimum event time, by contrast, would leave such changes on
 * the stream side while the JDBC read has already captured their effect, duplicating them.)
 */
public class KafkaJsonKafkaOffsetUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KafkaJsonKafkaOffsetUtils() {}

    /**
     * Returns the current position of the change-log stream: the maximum message event time across
     * all configured topics and partitions, stamped on the sentinel partition/offset (see the class
     * javadoc). Returns {@link KafkaJsonOffset#INITIAL_OFFSET} when no message is available yet.
     */
    public static KafkaJsonOffset queryCurrentOffset(KafkaJsonSourceConfig sourceConfig) {
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(buildConsumerProps(sourceConfig))) {
            return queryCurrentOffset(
                    consumer,
                    sourceConfig.getKafkaTopics(),
                    sourceConfig.getEventTime(),
                    sourceConfig.getMessageFormat());
        }
    }

    /**
     * Returns the current position of the change-log stream using the given (reusable) consumer.
     *
     * <p>For each partition the newest message is read and its event time for the configured mode is
     * extracted from the message content — Kafka's own record timestamp is the producer send time,
     * not the source change time, so it can not be used as the offset ordering key.
     */
    public static KafkaJsonOffset queryCurrentOffset(
            KafkaConsumer<String, String> consumer,
            List<String> topics,
            EventTime eventTime,
            MessageFormat format) {
        List<TopicPartition> partitions = listPartitions(consumer, topics);
        if (partitions.isEmpty()) {
            return KafkaJsonOffset.INITIAL_OFFSET;
        }

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long maxEventTime = Long.MIN_VALUE;
        boolean found = false;
        for (TopicPartition partition : partitions) {
            Long endOffset = endOffsets.get(partition);
            if (endOffset == null || endOffset <= 0) {
                // empty partition, nothing to read
                continue;
            }
            long lastOffset = endOffset - 1;
            consumer.assign(Collections.singleton(partition));
            consumer.seek(partition, lastOffset);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() == lastOffset) {
                    long et = extractEventTime(record.value(), eventTime, format);
                    if (et >= 0) {
                        maxEventTime = Math.max(maxEventTime, et);
                        found = true;
                    }
                }
            }
        }
        if (!found) {
            return KafkaJsonOffset.INITIAL_OFFSET;
        }
        // The watermark is the maximum event time of the partitions' newest messages, stamped onto
        // a
        // sentinel partition/offset. Real messages (partition < Integer.MAX_VALUE) with the same
        // event time therefore order BEFORE the watermark: the bounded backfill owns them
        // (inclusive
        // at the high watermark) and the stream phase emits only event times strictly after it.
        return new KafkaJsonOffset(maxEventTime, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    /** Extracts the event time (millis) of a canal flatMessage. Returns {@code -1} when absent. */
    public static long extractEventTime(String message, EventTime eventTime) {
        return extractEventTime(message, eventTime, MessageFormat.CANAL);
    }

    /**
     * Extracts the event time (millis) of a message for the configured {@link EventTime} mode and
     * message format. Returns {@code -1} if the message is null/empty, unparsable, or carries no
     * usable time.
     */
    public static long extractEventTime(
            String message, EventTime eventTime, MessageFormat format) {
        if (message == null || message.isEmpty()) {
            return -1L;
        }
        try {
            if (format == MessageFormat.DEBEZIUM) {
                // single source of truth for the extraction semantics: the parser binds the message
                // and getEventTimeValue applies the mode (source.ts_ms / payload.ts_ms / TSO)
                DebeziumMessage dbz = new DebeziumMessageParser().parse(message);
                if (dbz == null) {
                    return -1L;
                }
                Long value = dbz.getEventTimeValue(eventTime);
                return value == null ? -1L : value;
            }
            JsonNode root = OBJECT_MAPPER.readTree(message);
            String field;
            switch (eventTime) {
                case ES:
                case TIDB_TSO:
                    // A canal flatMessage carries no top-level TSO; the sampled watermark degrades
                    // to es (the commit-time equivalent). TiDB+TIDB_TSO uses the TSO query path of
                    // KafkaJsonTidbOffsetUtils instead of the Kafka sampling, so this branch is
                    // only a fallback for exotic canal shapes.
                    field = "es";
                    break;
                case TS:
                default:
                    field = "ts";
                    break;
            }
            JsonNode node = root.get(field);
            if (node == null || node.isNull()) {
                return -1L;
            }
            return node.asLong(-1L);
        } catch (Exception e) {
            return -1L;
        }
    }

    private static List<TopicPartition> listPartitions(
            KafkaConsumer<String, String> consumer, List<String> topics) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : topics) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null) {
                continue;
            }
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
        return partitions;
    }

    /** Builds the consumer properties for a dedicated, throw-away (non-group) consumer. */
    public static Properties buildConsumerProps(KafkaJsonSourceConfig sourceConfig) {
        return KafkaJsonKafkaUtils.buildConsumerProps(sourceConfig, null);
    }
}
