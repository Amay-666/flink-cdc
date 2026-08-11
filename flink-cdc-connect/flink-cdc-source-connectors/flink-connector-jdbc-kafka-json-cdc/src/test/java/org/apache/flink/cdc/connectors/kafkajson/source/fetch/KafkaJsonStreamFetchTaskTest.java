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

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.FakeKafkaConsumer;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonStreamFetchTask} (the streaming Kafka reader). */
class KafkaJsonStreamFetchTaskTest {

    private static final TopicPartition PARTITION = new TopicPartition("t", 0);

    private static final String INSERT =
            "{\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],\"database\":\"test\",\"es\":3000,"
                    + "\"id\":1,\"isDdl\":false,"
                    + "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"},"
                    + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                    + "\"table\":\"users\",\"ts\":3100,\"type\":\"INSERT\"}";
    private static final String DDL =
            "{\"data\":null,\"database\":\"test\",\"es\":3002,\"id\":2,\"isDdl\":true,"
                    + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                    + "\"sql\":\"ALTER TABLE `test`.`users` ADD COLUMN `age` int\","
                    + "\"sqlType\":null,\"table\":\"users\",\"ts\":3102,\"type\":\"ALTER\"}";
    private static final String UPDATE =
            "{\"data\":[{\"id\":\"1\",\"name\":\"Bob\"}],\"database\":\"test\",\"es\":3005,"
                    + "\"id\":3,\"isDdl\":false,"
                    + "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"},"
                    + "\"old\":[{\"name\":\"Alice\"}],\"pkNames\":[\"id\"],\"sql\":\"\","
                    + "\"sqlType\":{},\"table\":\"users\",\"ts\":3105,\"type\":\"UPDATE\"}";

    @Test
    void testConsumesConvertsAndTracksOffset() throws Exception {
        FakeKafkaConsumer consumer = consumer(INSERT, DDL, UPDATE);
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                streamSplit(new KafkaJsonOffset(3000, 0, 0), KafkaJsonOffset.NO_STOPPING_OFFSET);
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        Thread thread = new Thread(() -> runQuietly(task, context));
        thread.setDaemon(true);
        thread.start();
        try {
            List<SourceRecord> records = drain(context.getQueue(), 2);
            assertEquals(2, records.size());

            // INSERT -> op=c
            Struct first = (Struct) records.get(0).value();
            assertEquals("c", first.getString(Envelope.FieldName.OPERATION));
            assertEquals("Alice", first.getStruct(Envelope.FieldName.AFTER).getString("name"));
            assertEquals(1L, ((Struct) records.get(0).key()).getInt64("id"));
            assertEquals("t", records.get(0).topic());
            assertEquals(Integer.valueOf(0), records.get(0).kafkaPartition());

            // UPDATE -> op=u carrying before/after
            Struct second = (Struct) records.get(1).value();
            assertEquals("u", second.getString(Envelope.FieldName.OPERATION));
            assertEquals("Alice", second.getStruct(Envelope.FieldName.BEFORE).getString("name"));
            assertEquals("Bob", second.getStruct(Envelope.FieldName.AFTER).getString("name"));

            // the DDL message in between produced no record; the offset tracks the last message
            assertEquals(new KafkaJsonOffset(3005, 0, 2), task.getCurrentOffset());
            assertEquals(3L, consumer.positionOf(PARTITION));
        } finally {
            task.close();
            thread.join(5000);
        }
        assertTrue(consumer.isWakeupCalled());
        assertFalse(task.isRunning());
    }

    @Test
    void testBoundedReadDispatchesEndWatermark() throws Exception {
        FakeKafkaConsumer consumer = consumer(INSERT, DDL, UPDATE);
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                streamSplit(new KafkaJsonOffset(3000, 0, 0), new KafkaJsonOffset(3005, -1, -1));
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        // bounded read finishes on its own: after the last message the END watermark is dispatched
        task.execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 3);
        assertEquals(3, records.size());
        assertEquals("c", ((Struct) records.get(0).value()).getString(Envelope.FieldName.OPERATION));
        assertEquals("u", ((Struct) records.get(1).value()).getString(Envelope.FieldName.OPERATION));
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(2)));
        assertFalse(task.isRunning());
    }

    @Test
    void testInitialOffsetStartsFromEarliest() throws Exception {
        FakeKafkaConsumer consumer = consumer(INSERT);
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                streamSplit(KafkaJsonOffset.INITIAL_OFFSET, KafkaJsonOffset.NO_STOPPING_OFFSET);
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        Thread thread = new Thread(() -> runQuietly(task, context));
        thread.setDaemon(true);
        thread.start();
        try {
            List<SourceRecord> records = drain(context.getQueue(), 1);
            assertEquals(1, records.size());
            assertEquals("c", ((Struct) records.get(0).value()).getString(Envelope.FieldName.OPERATION));
            assertEquals(1L, consumer.positionOf(PARTITION));
        } finally {
            task.close();
            thread.join(5000);
        }
    }

    @Test
    void testCommitCurrentOffsetTracksLatest() {
        KafkaJsonStreamFetchTask task =
                new KafkaJsonStreamFetchTask(
                        streamSplit(KafkaJsonOffset.INITIAL_OFFSET, KafkaJsonOffset.NO_STOPPING_OFFSET));
        task.commitCurrentOffset(new KafkaJsonOffset(1000, 0, 5));
        assertEquals(new KafkaJsonOffset(1000, 0, 5), task.getLastCommittedOffset());
        // older offsets are not committed backwards
        task.commitCurrentOffset(new KafkaJsonOffset(900, 0, 3));
        assertEquals(new KafkaJsonOffset(1000, 0, 5), task.getLastCommittedOffset());
        task.commitCurrentOffset(new KafkaJsonOffset(1000, 1, 1));
        assertEquals(new KafkaJsonOffset(1000, 1, 1), task.getLastCommittedOffset());
    }

    private static KafkaJsonSourceFetchTaskContext context(FakeKafkaConsumer consumer) {
        KafkaJsonSourceConfig config = config();
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));
        context.setKafkaConsumerForTesting(consumer);
        return context;
    }

    private static KafkaJsonSourceConfig config() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .create(0);
    }

    private static StreamSplit streamSplit(KafkaJsonOffset startingOffset, KafkaJsonOffset endingOffset) {
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID,
                startingOffset,
                endingOffset,
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }

    /** Builds a single-partition consumer whose records carry ascending timestamps. */
    private static FakeKafkaConsumer consumer(String... messages) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int offset = 0; offset < messages.length; offset++) {
            // the record timestamp drives offsetsForTimes; the canal event time is inside the JSON
            records.add(
                    new ConsumerRecord<>(
                            PARTITION.topic(),
                            PARTITION.partition(),
                            offset,
                            3000L + offset,
                            TimestampType.CREATE_TIME,
                            -1L,
                            -1,
                            -1,
                            null,
                            messages[offset]));
        }
        log.put(PARTITION, records);
        return new FakeKafkaConsumer(log, null);
    }

    private static void runQuietly(KafkaJsonStreamFetchTask task, KafkaJsonSourceFetchTaskContext context) {
        try {
            task.execute(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Polls the queue until {@code expected} records have been drained (or a timeout elapses). */
    private static List<SourceRecord> drain(ChangeEventQueue<DataChangeEvent> queue, int expected)
            throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline && records.size() < expected) {
            for (DataChangeEvent event : queue.poll()) {
                records.add(event.getRecord());
            }
        }
        return records;
    }
}
