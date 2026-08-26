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

package org.apache.flink.cdc.connectors.kafkajson.source.message;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTableUtils;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Unit test for {@link KafkaJsonRecordFactory}. */
class KafkaJsonRecordFactoryTest {

    private final KafkaJsonSourceConfig config =
            new KafkaJsonSourceConfigFactory()
                    .hostname("localhost")
                    .username("root")
                    .password("x")
                    .databaseList("test")
                    .tableList("test.users")
                    .kafkaBootstrapServers("b")
                    .kafkaTopics("t")
                    .serverTimeZone("UTC")
                    .create(0);

    private final KafkaJsonRecordFactory factory = new KafkaJsonRecordFactory(config);
    private final MySqlConnectorConfig dbzConfig = config.getDbzConnectorConfig();

    private Table usersTable(String... pkNames) {
        Map<String, String> mysqlType = new java.util.LinkedHashMap<>();
        mysqlType.put("id", "bigint(20)");
        mysqlType.put("name", "varchar(255)");
        mysqlType.put("amount", "decimal(10,2)");
        mysqlType.put("created_at", "datetime");
        mysqlType.put("updated_at", "timestamp(3)");
        mysqlType.put("birth", "date");
        mysqlType.put("login_time", "time");
        mysqlType.put("enabled", "tinyint(1)");

        // LinkedHashMap keeps the column order deterministic (Jackson preserves JSON field order)
        Map<String, String> row = new java.util.LinkedHashMap<>();
        for (String col : mysqlType.keySet()) {
            row.put(col, "");
        }
        CanalMessage message = new CanalMessage();
        message.setDatabase("test");
        message.setTable("users");
        message.setPkNames(Arrays.asList(pkNames));
        message.setMysqlType(mysqlType);
        message.setData(Collections.singletonList(row));
        return KafkaJsonTableUtils.buildTable(message);
    }

    private Map<String, String> aliceRow() {
        Map<String, String> row = new HashMap<>();
        row.put("id", "1");
        row.put("name", "Alice");
        row.put("amount", "3.14");
        row.put("created_at", "2020-08-13 15:00:00");
        row.put("updated_at", "2020-08-13 15:00:00");
        row.put("birth", "2020-08-13");
        row.put("login_time", "15:03:02");
        row.put("enabled", "1");
        return row;
    }

    private KafkaJsonSourceInfo sourceInfo(SnapshotRecord snapshot) {
        return new KafkaJsonSourceInfo(
                dbzConfig,
                "test",
                "users",
                1598752886000L,
                1598752886000L,
                1598752887000L,
                snapshot);
    }

    @Test
    void testTypedValueConversionAndKeyStruct() {
        Table table = usersTable("id");
        factory.registerTable(table);
        Object[] data = factory.canalRowData(table, aliceRow());

        SourceRecord record =
                factory.createRecord(
                        table,
                        null,
                        data,
                        Envelope.Operation.CREATE,
                        sourceInfo(SnapshotRecord.FALSE),
                        "test.users",
                        0,
                        100L);

        // key struct contains only the primary key columns
        Struct key = (Struct) record.key();
        assertEquals(1L, key.getInt64("id"));
        assertEquals(1, key.schema().fields().size());

        // source partition / offset / topic
        assertEquals(
                Collections.singletonMap("server", "canal_cdc_source"), record.sourcePartition());
        Map<String, ?> offset = record.sourceOffset();
        assertEquals("1598752886000", offset.get("eventTime"));
        assertEquals("0", offset.get("partition"));
        assertEquals("100", offset.get("offset"));
        assertEquals("test.users", record.topic());
        assertEquals(Integer.valueOf(0), record.kafkaPartition());
        assertEquals(Long.valueOf(1598752886000L), record.timestamp());

        // envelope value
        Struct value = (Struct) record.value();
        assertEquals("c", value.getString("op"));
        Struct after = value.getStruct("after");
        assertEquals(1L, after.getInt64("id"));
        assertEquals("Alice", after.getString("name"));
        assertEquals(new BigDecimal("3.14"), after.get("amount"));
        // datetime with no fractional precision is emitted as epoch millis (Debezium ADAPTIVE mode)
        assertEquals(Long.valueOf(millisOf("2020-08-13 15:00:00")), after.getInt64("created_at"));
        assertEquals(LocalDate.parse("2020-08-13").toEpochDay(), (long) after.getInt32("birth"));
        assertEquals(
                Long.valueOf(
                        io.debezium.connector.mysql.MySqlValueConverters.stringToDuration(
                                                "15:03:02")
                                        .toNanos()
                                / 1_000L),
                after.getInt64("login_time"));
        assertEquals((short) 1, after.getInt16("enabled"));

        // MySQL TIMESTAMP is emitted as an ISO instant string in the server zone
        String updatedAt = after.getString("updated_at");
        assertEquals(Instant.parse("2020-08-13T15:00:00Z"), Instant.parse(updatedAt));

        // source struct
        Struct source = value.getStruct("source");
        assertEquals("canal", source.getString("connector"));
        assertEquals("canal_cdc_source", source.getString("name"));
        // SnapshotRecord.toSource is a no-op for FALSE -> stream records leave snapshot unset
        assertNull(source.getString("snapshot"));
        assertEquals("users", source.getString("table"));
    }

    @Test
    void testSnapshotAndStreamRecordsAreConsistent() {
        Table table = usersTable("id");
        factory.registerTable(table);
        Object[] data = factory.canalRowData(table, aliceRow());

        SourceRecord streamRecord =
                factory.createRecord(
                        table,
                        null,
                        data,
                        Envelope.Operation.CREATE,
                        sourceInfo(SnapshotRecord.FALSE),
                        "test.users",
                        0,
                        100L);
        SourceRecord snapshotRecord =
                factory.createRecord(
                        table,
                        null,
                        data,
                        Envelope.Operation.READ,
                        sourceInfo(SnapshotRecord.TRUE),
                        "test.users",
                        0,
                        100L);

        // identical value schema and identical after struct
        assertSame(streamRecord.valueSchema(), snapshotRecord.valueSchema());
        assertEquals(
                ((Struct) streamRecord.value()).getStruct("after"),
                ((Struct) snapshotRecord.value()).getStruct("after"));
        // different op and snapshot flag
        assertEquals("c", ((Struct) streamRecord.value()).getString("op"));
        assertEquals("r", ((Struct) snapshotRecord.value()).getString("op"));
        assertNull(((Struct) streamRecord.value()).getStruct("source").getString("snapshot"));
        assertEquals(
                "true",
                ((Struct) snapshotRecord.value()).getStruct("source").getString("snapshot"));
    }

    @Test
    void testUpdateRecordCarriesBeforeAndAfter() {
        Table table = usersTable("id");
        factory.registerTable(table);
        Map<String, String> oldRow = new HashMap<>();
        oldRow.put("name", "Alice");

        SourceRecord record =
                factory.createRecord(
                        table,
                        factory.canalRowData(table, oldRow),
                        factory.canalRowData(table, aliceRow()),
                        Envelope.Operation.UPDATE,
                        sourceInfo(SnapshotRecord.FALSE),
                        "test.users",
                        0,
                        100L);
        Struct value = (Struct) record.value();
        assertEquals("u", value.getString("op"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
        // columns not present in the canal `old` image are null in `before`
        assertNull(value.getStruct("before").get("id"));
        assertEquals("Alice", value.getStruct("after").getString("name"));
    }

    @Test
    void testDeleteRecordUsesBeforeForKey() {
        Table table = usersTable("id");
        factory.registerTable(table);
        SourceRecord record =
                factory.createRecord(
                        table,
                        factory.canalRowData(table, aliceRow()),
                        null,
                        Envelope.Operation.DELETE,
                        sourceInfo(SnapshotRecord.FALSE),
                        "test.users",
                        0,
                        100L);
        Struct value = (Struct) record.value();
        assertEquals("d", value.getString("op"));
        // key still resolvable from the before image
        assertEquals(1L, ((Struct) record.key()).getInt64("id"));
        assertNull(value.getStruct("after"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
    }

    @Test
    void testNoPrimaryKeyDegradesToNullKey() {
        Table table = usersTable();
        factory.registerTable(table);
        Object[] data = factory.canalRowData(table, aliceRow());
        SourceRecord record =
                factory.createRecord(
                        table,
                        null,
                        data,
                        Envelope.Operation.CREATE,
                        sourceInfo(SnapshotRecord.FALSE),
                        "test.users",
                        0,
                        100L);
        assertNull(record.key());
        assertNull(record.keySchema());
    }

    @Test
    void testKafkaJsonRowDataMissingColumnIsNull() {
        Table table = usersTable("id");
        factory.registerTable(table);
        Object[] data = factory.canalRowData(table, Collections.singletonMap("id", "7"));
        // column order follows the data row (id, name, ...); id passes through as a String for
        // signed BIGINT, columns absent from the row become null
        assertEquals("7", data[0]);
        assertNull(data[1]); // name
    }

    private static long millisOf(String datetime) {
        return Timestamp.valueOf(datetime).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000L;
    }
}
