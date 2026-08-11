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

package org.apache.flink.cdc.connectors.canal.source.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * An in-memory {@link KafkaConsumer} test double used to unit-test the streaming reader without a
 * Kafka broker.
 *
 * <p>Each partition is backed by an immutable log of {@link ConsumerRecord}s plus a mutable read
 * position. {@link #seek}/{@link #seekToBeginning}/{@link #seekToEnd} move the position; {@link
 * #poll(Duration)} returns the records of the currently assigned partitions that are at or after the
 * position and then advances it. {@link #offsetsForTimes(Map)} searches the log by the record
 * timestamp (which the tests set to the canal event time). {@link #endOffsets(Collection)} returns
 * the preconfigured log end (which may exceed the buffered records, as a real topic log would).
 *
 * <p>All Kafka-consumer methods the production code uses are overridden, so the real {@link
 * KafkaConsumer} constructor never touches a broker.
 */
public class FakeKafkaConsumer extends KafkaConsumer<String, String> {

    private final Map<String, List<PartitionInfo>> partitionsByTopic = new HashMap<>();
    private final Map<TopicPartition, List<ConsumerRecord<String, String>>> log =
            new LinkedHashMap<>();
    private final Map<TopicPartition, Long> endingOffsets = new HashMap<>();
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private final List<TopicPartition> assigned = new ArrayList<>();
    private volatile boolean wakeupCalled = false;

    public FakeKafkaConsumer(
            Map<TopicPartition, List<ConsumerRecord<String, String>>> log,
            Map<TopicPartition, Long> endingOffsets) {
        super(defaultProps());
        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, String>>> entry :
                log.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            this.log.put(topicPartition, entry.getValue());
            this.endingOffsets.put(
                    topicPartition,
                    endingOffsets == null
                            ? (long) entry.getValue().size()
                            : endingOffsets.getOrDefault(topicPartition, 0L));
            this.positions.put(topicPartition, 0L);
            this.partitionsByTopic
                    .computeIfAbsent(topicPartition.topic(), key -> new ArrayList<>())
                    .add(
                            new PartitionInfo(
                                    topicPartition.topic(),
                                    topicPartition.partition(),
                                    null,
                                    new Node[0],
                                    new Node[0]));
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition topicPartition : partitions) {
            result.put(
                    topicPartition,
                    endingOffsets.getOrDefault(topicPartition, (long) log.size()));
        }
        return result;
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        assigned.clear();
        assigned.addAll(partitions);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        positions.put(partition, Math.max(0L, offset));
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
            positions.put(topicPartition, 0L);
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
            positions.put(
                    topicPartition,
                    (long) log.getOrDefault(topicPartition, Collections.emptyList()).size());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long timestamp = entry.getValue();
            OffsetAndTimestamp found = null;
            for (ConsumerRecord<String, String> record :
                    log.getOrDefault(topicPartition, Collections.emptyList())) {
                if (record.timestamp() >= timestamp) {
                    found = new OffsetAndTimestamp(record.offset(), record.timestamp());
                    break;
                }
            }
            if (found != null) {
                result.put(topicPartition, found);
            }
        }
        return result;
    }

    @Override
    public ConsumerRecords<String, String> poll(Duration timeout) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> batch = new HashMap<>();
        for (TopicPartition topicPartition : assigned) {
            List<ConsumerRecord<String, String>> partitionLog =
                    log.getOrDefault(topicPartition, Collections.emptyList());
            long position = positions.getOrDefault(topicPartition, 0L);
            List<ConsumerRecord<String, String>> toReturn = new ArrayList<>();
            while (position < partitionLog.size()) {
                toReturn.add(partitionLog.get((int) position));
                position++;
            }
            positions.put(topicPartition, position);
            if (!toReturn.isEmpty()) {
                batch.put(topicPartition, toReturn);
            }
        }
        return new ConsumerRecords<>(batch);
    }

    @Override
    public void wakeup() {
        this.wakeupCalled = true;
    }

    @Override
    public void close() {
        // no-op
    }

    public boolean isWakeupCalled() {
        return wakeupCalled;
    }

    /** Returns the partitions the consumer was last {@link #assign}ed to. */
    public List<TopicPartition> getAssigned() {
        return assigned;
    }

    /** Returns the current read position of the given partition. */
    public long positionOf(TopicPartition topicPartition) {
        return positions.getOrDefault(topicPartition, 0L);
    }

    private static Properties defaultProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "fake-consumer-group");
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
