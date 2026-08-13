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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        // the stream reads strictly after its starting offset, so the boundary is below the first
        // message (es 3000); the stream split must emit every message in this test
        StreamSplit split =
                streamSplit(new KafkaJsonOffset(2999, -1, -1), KafkaJsonOffset.NO_STOPPING_OFFSET);
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
                backfillSplit(new KafkaJsonOffset(3000, 0, 0), new KafkaJsonOffset(3005, -1, -1));
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        // bounded read finishes on its own and emits only the messages strictly before the ending
        // offset: the UPDATE carries es == 3005 (the ending offset), so it belongs to the stream
        // phase and must not be duplicated here; the END watermark is dispatched instead
        task.execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 2);
        assertEquals(2, records.size());
        assertEquals("c", ((Struct) records.get(0).value()).getString(Envelope.FieldName.OPERATION));
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(1)));
        assertFalse(task.isRunning());
    }

    @Test
    void testBoundedReadKeepsMessagesBeforeEndingAndWaitsForLaggingPartition() throws Exception {
        // Partition 0 crosses the ending offset (es 4000) while partition 1 still has messages
        // before it (es 3400) that have not been polled yet. The bounded read must emit every
        // message before the ending offset from both partitions and only then dispatch END.
        TopicPartition p0 = new TopicPartition("t", 0);
        TopicPartition p1 = new TopicPartition("t", 1);
        List<ConsumerRecord<String, String>> log0 = new ArrayList<>();
        log0.add(insertRecord("1", "Alice", 3000, p0, 0));
        log0.add(insertRecord("2", "Bob", 4000, p0, 1));
        List<ConsumerRecord<String, String>> log1 = new ArrayList<>();
        log1.add(insertRecord("3", "Carol", 3100, p1, 0));
        log1.add(insertRecord("4", "Dave", 3400, p1, 1));

        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        log.put(p0, log0);
        log.put(p1, log1);
        FakeKafkaConsumer consumer = new FakeKafkaConsumer(log, null, 1);
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                backfillSplit(new KafkaJsonOffset(3000, -1, -1), new KafkaJsonOffset(3500, -1, -1));
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        task.execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 4);
        assertEquals(4, records.size(), "Alice/Carol/Dave before ending + END watermark");
        Set<String> names = new HashSet<>();
        for (SourceRecord record : records) {
            if (!WatermarkEvent.isEndWatermarkEvent(record)) {
                names.add(((Struct) record.value()).getStruct(Envelope.FieldName.AFTER).getString("name"));
            }
        }
        assertEquals(new HashSet<>(Arrays.asList("Alice", "Carol", "Dave")), names);
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(3)));
        assertFalse(task.isRunning());
    }

    @Test
    void testBoundedReadKeepsBoundaryMessageAtEndingOffset() throws Exception {
        // The user-observed duplicate: a change committed while the snapshot split's JDBC read is
        // running is the newest message in the topic, so its event time equals the split's high
        // watermark. The watermark carries the sentinel partition/offset, so the boundary message
        // is ordered BEFORE it (isAfter is false) and is emitted exactly once by this bounded
        // backfill; only messages strictly after the ending offset are left to the stream phase.
        // (With a minimum-event-time watermark the same boundary message fell through to the stream
        // phase and was emitted again, on top of a JDBC snapshot that already contained its effect.)
        FakeKafkaConsumer consumer = consumer(INSERT, UPDATE); // es 3000 then es 3005
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                backfillSplit(
                        new KafkaJsonOffset(3000, -1, -1),
                        new KafkaJsonOffset(3005, Integer.MAX_VALUE, Long.MAX_VALUE));
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        task.execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 3);
        assertEquals(3, records.size(), "INSERT + boundary UPDATE (es == ending) + END watermark");
        Struct update = (Struct) records.get(1).value();
        assertEquals("u", update.getString(Envelope.FieldName.OPERATION));
        assertEquals("Bob", update.getStruct(Envelope.FieldName.AFTER).getString("name"));
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(2)));
        assertFalse(task.isRunning());
    }

    @Test
    void testBoundedReadDropsMessagesBeforeStartingOffset() throws Exception {
        // A message committed before the snapshot but delivered to Kafka only after the seek
        // position (canal lag) carries an event time below the low watermark. The JDBC snapshot row
        // already reflects — and possibly supersedes — it, so replaying it would override the
        // snapshot with a stale value: it is dropped, while messages inside the (low, high] window
        // are still emitted. The Kafka timestamp of the lagged message is >= the seek timestamp so
        // the consumer actually reads it and the drop is what keeps it out of the backfill.
        TopicPartition p0 = new TopicPartition("t", 0);
        List<ConsumerRecord<String, String>> log0 = new ArrayList<>();
        log0.add(insertRecord("1", "Alice", 2900, 3000, p0, 0)); // pre-snapshot change, lagged: dropped
        log0.add(insertRecord("2", "Bob", 3200, 3200, p0, 1)); // inside the backfill window: emitted
        log0.add(insertRecord("3", "Carol", 3400, 3400, p0, 2)); // inside the backfill window: emitted

        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        log.put(p0, log0);
        FakeKafkaConsumer consumer = new FakeKafkaConsumer(log, null);
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                backfillSplit(
                        new KafkaJsonOffset(3000, -1, -1),
                        new KafkaJsonOffset(3500, Integer.MAX_VALUE, Long.MAX_VALUE));
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        task.execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 3);
        assertEquals(3, records.size(), "Bob + Carol inside the window + END watermark; Alice dropped");
        Set<String> names = new HashSet<>();
        for (SourceRecord record : records) {
            if (!WatermarkEvent.isEndWatermarkEvent(record)) {
                names.add(((Struct) record.value()).getStruct(Envelope.FieldName.AFTER).getString("name"));
            }
        }
        assertEquals(new HashSet<>(Arrays.asList("Bob", "Carol")), names);
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(2)));
        assertFalse(task.isRunning());
    }

    @Test
    void testStreamSplitDropsBoundaryMessageAtStartingOffset() throws Exception {
        // F1 regression (see docs/BOUNDARY_AUDIT.md): the newest message at the moment the snapshot
        // split finished carries an event time equal to the split's high watermark, so its event
        // time equals the stream split's starting offset (the high-watermark sentinel). The bounded
        // backfill emits it exactly once; the base pure-stream threshold is inclusive (isAtOrAfter),
        // so without the strict lower bound the stream re-emits it a second time. The stream split
        // reads strictly after its starting offset: the boundary INSERT (es == 3000) is consumed
        // (the read position advances) but not re-emitted, while the UPDATE strictly after it is.
        FakeKafkaConsumer consumer = consumer(INSERT, UPDATE); // es 3000 (boundary) then es 3005
        KafkaJsonSourceFetchTaskContext context = context(consumer);
        StreamSplit split =
                streamSplit(
                        new KafkaJsonOffset(3000, Integer.MAX_VALUE, Long.MAX_VALUE),
                        KafkaJsonOffset.NO_STOPPING_OFFSET);
        KafkaJsonStreamFetchTask task = new KafkaJsonStreamFetchTask(split);
        context.configure(split);

        Thread thread = new Thread(() -> runQuietly(task, context));
        thread.setDaemon(true);
        thread.start();
        try {
            List<SourceRecord> records = drain(context.getQueue(), 1);
            assertEquals(1, records.size(), "boundary INSERT at es == starting offset is dropped");
            Struct update = (Struct) records.get(0).value();
            assertEquals("u", update.getString(Envelope.FieldName.OPERATION));
            assertEquals("Bob", update.getStruct(Envelope.FieldName.AFTER).getString("name"));
            // the dropped boundary message still advanced the read position and offset tracking
            assertEquals(new KafkaJsonOffset(3005, 0, 1), task.getCurrentOffset());
            assertEquals(2L, consumer.positionOf(PARTITION));
        } finally {
            task.close();
            thread.join(5000);
        }
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

    /**
     * Builds a backfill split: the bounded re-read of a finished snapshot split, whose splitId is
     * the snapshot split id rather than {@link StreamSplit#STREAM_SPLIT_ID}. Unlike the stream
     * split, the backfill keeps inclusive bounds — it must emit the boundary message whose event
     * time equals the ending offset — so the stream-split exclusive lower bound must not apply.
     */
    private static StreamSplit backfillSplit(KafkaJsonOffset startingOffset, KafkaJsonOffset endingOffset) {
        return new StreamSplit(
                "snapshot-split-0",
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

    /** Builds an INSERT record whose canal event time (inside the JSON) equals its Kafka timestamp. */
    private static ConsumerRecord<String, String> insertRecord(
            String id, String name, long eventTime, TopicPartition partition, long offset) {
        return insertRecord(id, name, eventTime, eventTime, partition, offset);
    }

    /**
     * Builds an INSERT record with a distinct canal event time and Kafka timestamp. The Kafka
     * timestamp drives {@code offsetsForTimes} (the seek); the canal event time is the ordering key
     * used by the bounded read, so separating them simulates a message that canal committed before
     * the snapshot but delivered to Kafka only later (lag).
     */
    private static ConsumerRecord<String, String> insertRecord(
            String id,
            String name,
            long eventTime,
            long kafkaTimestamp,
            TopicPartition partition,
            long offset) {
        String json =
                String.format(
                        "{\"data\":[{\"id\":\"%s\",\"name\":\"%s\"}],\"database\":\"test\",\"es\":%d,"
                                + "\"id\":1,\"isDdl\":false,"
                                + "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"},"
                                + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                                + "\"table\":\"users\",\"ts\":%d,\"type\":\"INSERT\"}",
                        id, name, eventTime, kafkaTimestamp);
        return new ConsumerRecord<>(
                partition.topic(),
                partition.partition(),
                offset,
                kafkaTimestamp,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                null,
                json);
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
