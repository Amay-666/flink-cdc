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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessageParser;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the Debezium DML conversion path of {@link KafkaJsonRecordConverter}: typed {@code
 * before}/{@code after} images converted against the registered (JDBC) table schema (see
 * docs/DEBEZIUM_PLAN.md §S3).
 */
class DebeziumRecordConverterTest {

    private static final DebeziumMessageParser PARSER = new DebeziumMessageParser();

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
                    .messageFormat(KafkaJsonSourceOptions.MessageFormat.DEBEZIUM)
                    .create(0);

    private final KafkaJsonRecordFactory factory = new KafkaJsonRecordFactory(config);

    /** The table schema the snapshot phase would have registered for {@code test.users}. */
    private Table registerUsersTable(KafkaJsonRecordFactory factory) {
        Table table =
                Table.editor()
                        .tableId(new TableId("test", null, "users"))
                        .addColumn(
                                Column.editor()
                                        .name("id")
                                        .jdbcType(Types.BIGINT)
                                        .type("BIGINT")
                                        .length(20)
                                        .create())
                        .addColumn(
                                Column.editor()
                                        .name("name")
                                        .jdbcType(Types.VARCHAR)
                                        .type("VARCHAR")
                                        .length(255)
                                        .optional(true)
                                        .create())
                        .addColumn(
                                Column.editor()
                                        .name("amount")
                                        .jdbcType(Types.DECIMAL)
                                        .type("DECIMAL")
                                        .length(10)
                                        .scale(2)
                                        .optional(true)
                                        .create())
                        .addColumn(
                                Column.editor()
                                        .name("birth")
                                        .jdbcType(Types.DATE)
                                        .type("DATE")
                                        .length(10)
                                        .optional(true)
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();
        factory.registerTable(table);
        return table;
    }

    private Table registerUsersTable() {
        return registerUsersTable(factory);
    }

    private KafkaJsonRecordConverter converter() {
        return new KafkaJsonRecordConverter(factory, config);
    }

    private static DebeziumMessage parse(String json) {
        return PARSER.parse(json);
    }

    @Test
    void testInsert() {
        registerUsersTable();
        List<SourceRecord> records =
                converter()
                        .convert(
                                parse(
                                        "{\"payload\":{"
                                                + "\"before\":null,"
                                                + "\"after\":{\"id\":1,\"name\":\"Alice\","
                                                + "\"amount\":123.45,\"birth\":18487},"
                                                + "\"source\":{\"connector\":\"mysql\","
                                                + "\"ts_ms\":1598752886000,\"db\":\"test\","
                                                + "\"table\":\"users\"},"
                                                + "\"op\":\"c\",\"ts_ms\":1598752887000}}"),
                                "test.users",
                                0,
                                100L);

        assertEquals(1, records.size());
        SourceRecord record = records.get(0);
        Struct value = (Struct) record.value();
        assertEquals("c", value.getString("op"));
        Struct after = value.getStruct("after");
        assertEquals("Alice", after.getString("name"));
        // a DECIMAL delivered as a float64 and a DATE delivered as epoch days both convert
        assertEquals(new java.math.BigDecimal("123.45"), after.get("amount"));
        assertEquals(
                Integer.valueOf((int) LocalDate.of(2020, 8, 13).toEpochDay()),
                after.getInt32("birth"));
        // key resolved from the after image
        assertEquals(1L, ((Struct) record.key()).getInt64("id"));
        // ES mode: the offset event time and source.es come from source.ts_ms
        assertEquals("1598752886000", record.sourceOffset().get("eventTime"));
        Struct source = value.getStruct("source");
        assertEquals(1598752886000L, source.getInt64("es"));
        assertEquals(1598752887000L, source.getInt64("ts"));
    }

    @Test
    void testUpdateCarriesBeforeAndAfter() {
        registerUsersTable();
        List<SourceRecord> records =
                converter()
                        .convert(
                                parse(
                                        "{\"payload\":{"
                                                + "\"before\":{\"id\":1,\"name\":\"Alice\"},"
                                                + "\"after\":{\"id\":1,\"name\":\"Bob\"},"
                                                + "\"source\":{\"ts_ms\":10,\"db\":\"test\","
                                                + "\"table\":\"users\"},"
                                                + "\"op\":\"u\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L);

        assertEquals(1, records.size());
        Struct value = (Struct) records.get(0).value();
        assertEquals("u", value.getString("op"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
        assertEquals("Bob", value.getStruct("after").getString("name"));
        assertEquals(1L, ((Struct) records.get(0).key()).getInt64("id"));
    }

    @Test
    void testDeleteUsesBeforeImage() {
        registerUsersTable();
        List<SourceRecord> records =
                converter()
                        .convert(
                                parse(
                                        "{\"payload\":{"
                                                + "\"before\":{\"id\":1,\"name\":\"Alice\"},"
                                                + "\"after\":null,"
                                                + "\"source\":{\"ts_ms\":10,\"db\":\"test\","
                                                + "\"table\":\"users\"},"
                                                + "\"op\":\"d\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L);

        assertEquals(1, records.size());
        Struct value = (Struct) records.get(0).value();
        assertEquals("d", value.getString("op"));
        assertNull(value.getStruct("after"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
        // key resolved from the (before) image
        assertEquals(1L, ((Struct) records.get(0).key()).getInt64("id"));
    }

    @Test
    void testReadOpMapsToEnvelopeRead() {
        registerUsersTable();
        List<SourceRecord> records =
                converter()
                        .convert(
                                parse(
                                        "{\"payload\":{"
                                                + "\"before\":null,"
                                                + "\"after\":{\"id\":1,\"name\":\"Alice\"},"
                                                + "\"source\":{\"ts_ms\":10,\"db\":\"test\","
                                                + "\"table\":\"users\"},"
                                                + "\"op\":\"r\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L);
        assertEquals(1, records.size());
        assertEquals("r", ((Struct) records.get(0).value()).getString("op"));
    }

    @Test
    void testWatermarkAndUnknownOpsProduceNoRecords() {
        registerUsersTable();
        KafkaJsonRecordConverter converter = converter();
        // TiDB watermark marker
        assertTrue(
                converter
                        .convert(
                                parse(
                                        "{\"payload\":{"
                                                + "\"before\":null,\"after\":null,"
                                                + "\"source\":{\"connector\":\"tidb\","
                                                + "\"commit_ts\":1,\"cluster_id\":\"c1\"},"
                                                + "\"op\":\"m\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L)
                        .isEmpty());
        // unknown op
        assertTrue(
                converter
                        .convert(
                                parse(
                                        "{\"payload\":{\"after\":{\"id\":1},"
                                                + "\"source\":{\"db\":\"test\",\"table\":\"users\"},"
                                                + "\"op\":\"x\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L)
                        .isEmpty());
    }

    @Test
    void testUnregisteredTableDropsMessage() {
        // a Debezium message cannot rebuild its schema (no mysqlType), so without the snapshot
        // phase's registration the row is dropped, not mis-decoded
        List<SourceRecord> records =
                converter()
                        .convert(
                                parse(
                                        "{\"payload\":{\"after\":{\"id\":1},"
                                                + "\"source\":{\"db\":\"test\",\"table\":\"users\"},"
                                                + "\"op\":\"c\",\"ts_ms\":20}}"),
                                "test.users",
                                0,
                                100L);
        assertTrue(records.isEmpty());
    }

    @Test
    void testEventTimeModeUsesTs() {
        registerUsersTable();
        KafkaJsonSourceConfig tsConfig =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("b")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .messageFormat(KafkaJsonSourceOptions.MessageFormat.DEBEZIUM)
                        .eventTime(KafkaJsonSourceOptions.EventTime.TS)
                        .create(0);
        KafkaJsonRecordFactory tsFactory = new KafkaJsonRecordFactory(tsConfig);
        registerUsersTable(tsFactory);
        List<SourceRecord> records =
                new KafkaJsonRecordConverter(tsFactory, tsConfig)
                        .convert(
                                parse(
                                        "{\"payload\":{\"after\":{\"id\":1,\"name\":\"Alice\"},"
                                                + "\"source\":{\"ts_ms\":1598752886000,"
                                                + "\"db\":\"test\",\"table\":\"users\"},"
                                                + "\"op\":\"c\",\"ts_ms\":1598752887000}}"),
                                "test.users",
                                0,
                                100L);
        assertEquals("1598752887000", records.get(0).sourceOffset().get("eventTime"));
    }
}
