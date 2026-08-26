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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.FakeKafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link KafkaJsonOffsetSupplier}. */
class KafkaJsonOffsetSupplierTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("t", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("t", 1);

    @Test
    void testCurrentReturnsMaxEventTimeAcrossPartitionsOnSentinel() {
        // partition 0: 5 messages, last one at es=3000 / ts=3500
        // partition 1: 3 messages, last one at es=2500 / ts=2800
        KafkaJsonOffsetSupplier supplier =
                new KafkaJsonOffsetSupplier(config(EventTime.ES), consumer(3000L, 2500L));
        // the max of the partitions' newest event times, stamped on the sentinel partition/offset
        // so that real messages with the same event time order BEFORE the watermark
        assertEquals(
                new KafkaJsonOffset(3000L, Integer.MAX_VALUE, Long.MAX_VALUE), supplier.current());
    }

    @Test
    void testCurrentWithTsMode() {
        // same es as the ES-mode test: partition 0 last es=3000 -> ts=3500, partition 1 last
        // es=2500 -> ts=2800, so the max send time is 3500
        KafkaJsonOffsetSupplier supplier =
                new KafkaJsonOffsetSupplier(config(EventTime.TS), consumer(3000L, 2500L));
        assertEquals(
                new KafkaJsonOffset(3500L, Integer.MAX_VALUE, Long.MAX_VALUE), supplier.current());
    }

    @Test
    void testCurrentOnEmptyTopicsReturnsInitialOffset() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        Map<TopicPartition, Long> endingOffsets = new HashMap<>();
        KafkaJsonOffsetSupplier supplier =
                new KafkaJsonOffsetSupplier(
                        config(EventTime.ES), new FakeKafkaConsumer(log, endingOffsets));
        assertEquals(KafkaJsonOffset.INITIAL_OFFSET, supplier.current());
    }

    @Test
    void testCurrentOnEmptyPartitionsReturnsInitialOffset() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        Map<TopicPartition, Long> endingOffsets = new HashMap<>();
        // partitions exist but their logs are empty (end offset 0)
        log.put(PARTITION_0, new ArrayList<>());
        endingOffsets.put(PARTITION_0, 0L);
        KafkaJsonOffsetSupplier supplier =
                new KafkaJsonOffsetSupplier(
                        config(EventTime.ES), new FakeKafkaConsumer(log, endingOffsets));
        assertEquals(KafkaJsonOffset.INITIAL_OFFSET, supplier.current());
    }

    private static KafkaJsonSourceConfig config(EventTime eventTime) {
        KafkaJsonSourceConfigFactory factory =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .eventTime(eventTime);
        return factory.create(0);
    }

    /** Builds a consumer whose two partitions hold 5 and 3 messages respectively. */
    private static FakeKafkaConsumer consumer(long lastEsPartition0, long lastEsPartition1) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        Map<TopicPartition, Long> endingOffsets = new HashMap<>();

        List<ConsumerRecord<String, String>> partition0Log = new ArrayList<>();
        for (long offset = 0; offset < 5; offset++) {
            long es = offset == 4 ? lastEsPartition0 : 1000L + offset;
            partition0Log.add(record(PARTITION_0, offset, es, es + 500L));
        }
        log.put(PARTITION_0, partition0Log);
        endingOffsets.put(PARTITION_0, 5L);

        List<ConsumerRecord<String, String>> partition1Log = new ArrayList<>();
        for (long offset = 0; offset < 3; offset++) {
            long es = offset == 2 ? lastEsPartition1 : 2000L + offset;
            partition1Log.add(record(PARTITION_1, offset, es, es + 300L));
        }
        log.put(PARTITION_1, partition1Log);
        endingOffsets.put(PARTITION_1, 3L);

        return new FakeKafkaConsumer(log, endingOffsets);
    }

    private static ConsumerRecord<String, String> record(
            TopicPartition topicPartition, long offset, long es, long ts) {
        String value =
                "{\"data\":[],\"database\":\"test\",\"es\":"
                        + es
                        + ",\"id\":1,"
                        + "\"isDdl\":false,\"mysqlType\":{},\"old\":null,\"pkNames\":null,"
                        + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\",\"ts\":"
                        + ts
                        + ",\"type\":\"INSERT\"}";
        return new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                offset,
                es,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                null,
                value);
    }
}
