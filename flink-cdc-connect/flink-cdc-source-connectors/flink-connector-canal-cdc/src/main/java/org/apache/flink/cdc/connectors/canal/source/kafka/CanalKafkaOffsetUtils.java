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

package org.apache.flink.cdc.connectors.canal.source.kafka;

import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffset;
import org.apache.flink.cdc.connectors.canal.source.utils.CanalKafkaUtils;

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
 * <p>Unlike a binlog position, there is no single "current offset" of the change log stream: each
 * Kafka partition has its own position. We therefore define the current position as the <b>minimum
 * message event time across all partitions</b>, which is a conservative lower bound: any change
 * generated before that event time has certainly been written to Kafka, while changes generated
 * after it will be read later in the stream phase. This keeps the full-&gt;incremental switch
 * lossless (at the cost of possible duplicate replay, which is deduplicated by the high watermark).
 */
public class CanalKafkaOffsetUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CanalKafkaOffsetUtils() {}

    /**
     * Returns the current position of the change-log stream: the minimum message event time across
     * all configured topics and partitions. Returns {@link CanalOffset#INITIAL_OFFSET} when no
     * message is available yet.
     */
    public static CanalOffset queryCurrentOffset(CanalSourceConfig sourceConfig) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProps(sourceConfig))) {
            return queryCurrentOffset(consumer, sourceConfig.getKafkaTopics(), sourceConfig.getEventTime());
        }
    }

    /**
     * Returns the current position of the change-log stream using the given (reusable) consumer.
     *
     * <p>For each partition the newest message is read and its canal event time (the configured
     * {@code es}/{@code ts} field) is extracted from the message content — Kafka's own record
     * timestamp is the producer send time, not the binlog execution time, so it can not be used as
     * the offset ordering key.
     */
    public static CanalOffset queryCurrentOffset(
            KafkaConsumer<String, String> consumer, List<String> topics, EventTime eventTime) {
        List<TopicPartition> partitions = listPartitions(consumer, topics);
        if (partitions.isEmpty()) {
            return CanalOffset.INITIAL_OFFSET;
        }

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long minEventTime = Long.MAX_VALUE;
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
                    long et = extractEventTime(record.value(), eventTime);
                    if (et >= 0) {
                        minEventTime = Math.min(minEventTime, et);
                        found = true;
                    }
                }
            }
        }
        if (!found) {
            return CanalOffset.INITIAL_OFFSET;
        }
        // the position is not bound to any specific partition
        return new CanalOffset(minEventTime, -1, -1L);
    }

    /**
     * Extracts the event time (millis) from a canal flatMessage JSON string. Returns {@code -1} if
     * the field is absent or unparsable.
     */
    public static long extractEventTime(String message, EventTime eventTime) {
        if (message == null || message.isEmpty()) {
            return -1L;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(message);
            String field = eventTime == EventTime.ES ? "es" : "ts";
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
    public static Properties buildConsumerProps(CanalSourceConfig sourceConfig) {
        return CanalKafkaUtils.buildConsumerProps(sourceConfig, null);
    }
}
