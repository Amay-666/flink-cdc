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

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonTiDBDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.FakeKafkaConsumer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link KafkaJsonScanFetchTask}: the snapshot read task (JDBC rows -&gt; READ
 * records) and the full incremental-snapshot pipeline (LOW -&gt; snapshot -&gt; HIGH -&gt; backfill
 * -&gt; END) driven by the base framework.
 *
 * <p>The JDBC result is faked with {@link java.lang.reflect.Proxy} handlers (the {@link
 * Connection}/ {@link PreparedStatement}/{@link ResultSet} interfaces are too large to implement by
 * hand); the Kafka change log is faked with {@link FakeKafkaConsumer}; the two watermark reads of
 * the base algorithm are faked with the dialect's offset supplier.
 */
class KafkaJsonScanFetchTaskTest {

    private static final TopicPartition PARTITION = new TopicPartition("t", 0);
    private static final TableId TABLE_ID = new TableId("test", null, "users");
    private static final RowType SPLIT_KEY_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("id", new BigIntType())));

    @Test
    void testExecuteRunsFullIncrementalSnapshotPipeline() throws Exception {
        // the snapshot rows read from MySQL via JDBC
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {1L, "A"});
        rows.add(new Object[] {3L, "D"});

        // the change log already written by canal to Kafka: an insert at the low watermark, a DDL
        // message between the watermarks, and an insert at the high watermark (es == 3000 == HIGH)
        FakeKafkaConsumer consumer =
                consumer(
                        new Object[] {1000L, insertMessage(1000, "1", "B")},
                        new Object[] {2000L, ddlMessage(2000)},
                        new Object[] {3000L, insertMessage(3000, "2", "C")});

        // The full pipeline (LOW -> snapshot -> HIGH -> backfill -> END) only runs when the
        // backfill is enabled, which is now the TiDB default (MySQL skips it, see
        // KafkaJsonSourceConfig#isSkipSnapshotBackfill). The boundary messages are TiDB-style
        // canal-json flatMessages (the `_tidb` extension is not needed for the offset order).
        KafkaJsonSourceConfig config = tidbConfig();
        KafkaJsonTiDBDialect dialect = new KafkaJsonTiDBDialect(config);
        AtomicInteger watermarkCalls = new AtomicInteger();
        dialect.setCurrentOffsetSupplierForTesting(
                () ->
                        watermarkCalls.getAndIncrement() == 0
                                ? new KafkaJsonOffset(1000, -1, -1)
                                : new KafkaJsonOffset(3000, -1, -1));
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, dialect);
        context.setKafkaConsumerForTesting(consumer);
        context.setJdbcConnectionForTesting(fakeJdbc(rows, new ArrayList<>(), new ArrayList<>()));

        // single-chunk split: no WHERE clause, the whole table is one chunk
        SnapshotSplit split = snapshotSplit(null, null);
        context.configure(split);

        new KafkaJsonScanFetchTask(split).execute(context);

        List<SourceRecord> records = drain(context.getQueue(), 6);
        assertEquals(6, records.size());

        // 1. low watermark: the stream position captured before the snapshot read
        assertTrue(WatermarkEvent.isLowWatermarkEvent(records.get(0)));
        assertEquals(new KafkaJsonOffset(1000, -1, -1).getOffset(), records.get(0).sourceOffset());

        // 2-3. the snapshot rows as READ records
        assertReadRecord(records.get(1), 1L, "A");
        assertReadRecord(records.get(2), 3L, "D");

        // 4. high watermark: the stream position captured after the snapshot read
        assertTrue(WatermarkEvent.isHighWatermarkEvent(records.get(3)));
        assertEquals(new KafkaJsonOffset(3000, -1, -1).getOffset(), records.get(3).sourceOffset());

        // 5. the backfill replayed (LOW, HIGH): the id=1 insert (es == LOW) overrides the snapshot
        // row of the same key via the base framework's output-buffer rewrite
        assertChangeRecord(records.get(4), 1L, "B");

        // the id=2 insert carries es == 3000 == the high watermark: this injected ending offset is
        // (3000, -1, -1), not the sentinel watermark of queryCurrentOffset, so the record is
        // ordered
        // AFTER it (partition 0 > -1) and dropped by the bounded read — the stream phase emits it

        // 6. the end watermark finalizes the split
        assertTrue(WatermarkEvent.isEndWatermarkEvent(records.get(5)));
        assertEquals(new KafkaJsonOffset(3000, -1, -1).getOffset(), records.get(5).sourceOffset());

        assertEquals(2, watermarkCalls.get());
    }

    @Test
    void testSnapshotSplitReadTaskEmitsReadRecords() throws Exception {
        // a middle chunk: splitStart=1, splitEnd=3 -> WHERE id >= ? AND NOT (id = ?) AND id <= ?
        SnapshotSplit split = snapshotSplit(new Object[] {1L}, new Object[] {3L});
        KafkaJsonSourceFetchTaskContext context = context();
        List<String> sqlLog = new ArrayList<>();
        List<Object[]> setObjectArgs = new ArrayList<>();
        context.setJdbcConnectionForTesting(
                fakeJdbc(Collections.singletonList(new Object[] {2L, "B"}), sqlLog, setObjectArgs));
        context.configure(split);

        KafkaJsonScanFetchTask.KafkaJsonSnapshotSplitReadTask readTask =
                new KafkaJsonScanFetchTask.KafkaJsonSnapshotSplitReadTask(
                        context.getRecordFactory(),
                        context.getDatabaseSchema(),
                        context.getConnection(),
                        context.getDbzConnectorConfig(),
                        context.getSourceConfig(),
                        context.getQueue(),
                        split);
        readTask.execute(() -> true);

        List<SourceRecord> records = drain(context.getQueue(), 1);
        assertEquals(1, records.size());
        assertEquals(
                "SELECT * FROM `test`.`users` WHERE id >= ? AND NOT (id = ?) AND id <= ?",
                sqlLog.get(0));
        // a middle chunk binds the lower bound once and the upper bound twice
        assertEquals(3, setObjectArgs.size());
        assertEquals(1, ((Integer) setObjectArgs.get(0)[0]).intValue());
        assertEquals(1L, setObjectArgs.get(0)[1]);
        assertEquals(2, ((Integer) setObjectArgs.get(1)[0]).intValue());
        assertEquals(3L, setObjectArgs.get(1)[1]);
        assertEquals(3, ((Integer) setObjectArgs.get(2)[0]).intValue());
        assertEquals(3L, setObjectArgs.get(2)[1]);

        SourceRecord record = records.get(0);
        Struct value = (Struct) record.value();
        assertEquals("r", value.getString(Envelope.FieldName.OPERATION));
        assertEquals(Long.valueOf(2L), ((Struct) record.key()).getInt64("id"));
        assertEquals("B", value.getStruct(Envelope.FieldName.AFTER).getString("name"));
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        assertEquals("test", source.getString("db"));
        assertEquals("users", source.getString("table"));
        assertEquals("true", source.getString("snapshot"));
        assertEquals("t", record.topic());
        assertEquals(Integer.valueOf(0), record.kafkaPartition());
    }

    private static void assertReadRecord(SourceRecord record, long id, String name) {
        Struct value = (Struct) record.value();
        assertEquals("r", value.getString(Envelope.FieldName.OPERATION));
        assertEquals(Long.valueOf(id), ((Struct) record.key()).getInt64("id"));
        assertEquals(name, value.getStruct(Envelope.FieldName.AFTER).getString("name"));
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        assertEquals("test", source.getString("db"));
        assertEquals("users", source.getString("table"));
        assertEquals("true", source.getString("snapshot"));
        assertEquals("t", record.topic());
        assertEquals(Integer.valueOf(0), record.kafkaPartition());
    }

    private static void assertChangeRecord(SourceRecord record, long id, String name) {
        Struct value = (Struct) record.value();
        assertEquals("c", value.getString(Envelope.FieldName.OPERATION));
        assertEquals(Long.valueOf(id), ((Struct) record.key()).getInt64("id"));
        assertEquals(name, value.getStruct(Envelope.FieldName.AFTER).getString("name"));
    }

    private static KafkaJsonSourceFetchTaskContext context() {
        KafkaJsonSourceConfig config = config();
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));
        context.setKafkaConsumerForTesting(new FakeKafkaConsumer(new HashMap<>(), new HashMap<>()));
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

    /**
     * A TiDB config: the only database type whose snapshot backfill is enabled by default (MySQL
     * always skips it), so the full incremental-snapshot pipeline can be exercised.
     */
    private static KafkaJsonSourceConfig tidbConfig() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .databaseType(KafkaJsonSourceOptions.DatabaseType.TIDB)
                .create(0);
    }

    private static SnapshotSplit snapshotSplit(Object[] splitStart, Object[] splitEnd) {
        Map<TableId, TableChange> tableSchemas =
                Collections.singletonMap(
                        TABLE_ID, new TableChange(TableChangeType.CREATE, table()));
        return new SnapshotSplit(
                TABLE_ID,
                "users-split-0",
                SPLIT_KEY_TYPE,
                splitStart,
                splitEnd,
                null,
                tableSchemas);
    }

    private static Table table() {
        return Table.editor()
                .tableId(TABLE_ID)
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("BIGINT")
                                .jdbcType(Types.BIGINT)
                                .length(20)
                                .optional(false)
                                .position(1)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("name")
                                .type("VARCHAR")
                                .jdbcType(Types.VARCHAR)
                                .length(255)
                                .optional(true)
                                .position(2)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    /**
     * Builds a {@link JdbcConnection} whose {@link Connection}/{@link PreparedStatement}/{@link
     * ResultSet} are faked with {@link Proxy} handlers: {@code prepareStatement} records the SQL in
     * {@code sqlLog}, {@code setObject} records its arguments in {@code setObjectArgs}, and {@code
     * executeQuery} iterates over {@code rows}.
     */
    private static JdbcConnection fakeJdbc(
            List<Object[]> rows, List<String> sqlLog, List<Object[]> setObjectArgs) {
        AtomicInteger rowIndex = new AtomicInteger(-1);
        ResultSet resultSet =
                (ResultSet)
                        Proxy.newProxyInstance(
                                ResultSet.class.getClassLoader(),
                                new Class<?>[] {ResultSet.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "next":
                                            return rowIndex.incrementAndGet() < rows.size();
                                        case "getObject":
                                            return args[0] instanceof Integer
                                                    ? rows.get(rowIndex.get())[
                                                            ((Integer) args[0]) - 1]
                                                    : null;
                                        case "close":
                                            return null;
                                        default:
                                            return defaultValue(method.getReturnType());
                                    }
                                });
        PreparedStatement preparedStatement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                PreparedStatement.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "setFetchSize":
                                            return null;
                                        case "setObject":
                                            setObjectArgs.add(Arrays.copyOf(args, args.length));
                                            return null;
                                        case "executeQuery":
                                            return resultSet;
                                        case "close":
                                            return null;
                                        default:
                                            return defaultValue(method.getReturnType());
                                    }
                                });
        Connection connection =
                (Connection)
                        Proxy.newProxyInstance(
                                Connection.class.getClassLoader(),
                                new Class<?>[] {Connection.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "prepareStatement":
                                            sqlLog.add((String) args[0]);
                                            return preparedStatement;
                                        case "setAutoCommit":
                                        case "close":
                                        case "rollback":
                                            return null;
                                        case "isClosed":
                                            return false;
                                        case "getAutoCommit":
                                            return false;
                                        default:
                                            return defaultValue(method.getReturnType());
                                    }
                                });
        return new JdbcConnection(JdbcConfiguration.empty(), config -> connection, "`", "`");
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0.0d;
        }
        if (type == float.class) {
            return 0.0f;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        return null;
    }

    /** Builds a single-partition consumer whose records carry the given (es, message) pairs. */
    private static FakeKafkaConsumer consumer(Object[]... esAndMessage) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> log = new HashMap<>();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int offset = 0; offset < esAndMessage.length; offset++) {
            long es = (Long) esAndMessage[offset][0];
            // the record timestamp drives offsetsForTimes; it must match the canal event time
            records.add(
                    new ConsumerRecord<>(
                            PARTITION.topic(),
                            PARTITION.partition(),
                            offset,
                            es,
                            TimestampType.CREATE_TIME,
                            -1L,
                            -1,
                            -1,
                            null,
                            (String) esAndMessage[offset][1]));
        }
        log.put(PARTITION, records);
        return new FakeKafkaConsumer(log, null);
    }

    private static String insertMessage(long es, String id, String name) {
        return "{\"data\":[{\"id\":\""
                + id
                + "\",\"name\":\""
                + name
                + "\"}],"
                + "\"database\":\"test\",\"es\":"
                + es
                + ",\"id\":1,\"isDdl\":false,"
                + "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"},"
                + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                + "\"table\":\"users\",\"ts\":"
                + (es + 500)
                + ",\"type\":\"INSERT\"}";
    }

    private static String ddlMessage(long es) {
        return "{\"data\":null,\"database\":\"test\",\"es\":"
                + es
                + ",\"id\":2,"
                + "\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                + "\"sql\":\"ALTER TABLE `test`.`users` ADD COLUMN `age` int\","
                + "\"sqlType\":null,\"table\":\"users\",\"ts\":"
                + (es + 500)
                + ",\"type\":\"ALTER\"}";
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
