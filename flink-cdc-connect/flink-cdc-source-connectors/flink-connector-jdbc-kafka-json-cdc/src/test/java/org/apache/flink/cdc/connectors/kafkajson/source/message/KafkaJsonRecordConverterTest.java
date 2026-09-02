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
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonRecordConverter}. */
class KafkaJsonRecordConverterTest {

    private static final String MYSQL_TYPE =
            "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"}";

    private KafkaJsonRecordConverter converter(boolean tsMode) {
        KafkaJsonSourceConfigFactory factory =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("b")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC");
        if (tsMode) {
            factory.eventTime(KafkaJsonSourceOptions.EventTime.TS);
        }
        KafkaJsonSourceConfig config = factory.create(0);
        return new KafkaJsonRecordConverter(new KafkaJsonRecordFactory(config), config);
    }

    @Test
    void testInsertWithMultipleRows() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"},"
                                                + "{\"id\":\"2\",\"name\":\"Bob\"}],"
                                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":1,"
                                                + "\"isDdl\":false,"
                                                + MYSQL_TYPE
                                                + ",\"old\":null,\"pkNames\":[\"id\"],"
                                                + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                                                + "\"ts\":1598752887000,\"type\":\"INSERT\"}"),
                        "test.users",
                        0,
                        100L);

        assertEquals(2, records.size());
        SourceRecord first = records.get(0);
        Struct value = (Struct) first.value();
        assertEquals("c", value.getString("op"));
        assertEquals("Alice", value.getStruct("after").getString("name"));
        assertEquals(1L, ((Struct) first.key()).getInt64("id"));
        assertEquals("test.users", first.topic());
        assertEquals(Integer.valueOf(0), first.kafkaPartition());

        // eventTime defaults to ES -> ts_ms and the kafka offset event time come from `es`
        assertEquals("1598752886000", first.sourceOffset().get("eventTime"));
        assertEquals(Long.valueOf(1598752886000L), first.timestamp());
        Struct source = value.getStruct("source");
        assertEquals(1598752886000L, source.getInt64("es"));
        assertEquals(1598752887000L, source.getInt64("ts"));

        // each row carries the same Kafka offset but a distinct key
        SourceRecord second = records.get(1);
        assertEquals(2L, ((Struct) second.key()).getInt64("id"));
        assertEquals("100", second.sourceOffset().get("offset"));
        assertEquals("1598752886000", second.sourceOffset().get("eventTime"));
        assertEquals("0", second.sourceOffset().get("partition"));
    }

    @Test
    void testEventTimeModeUsesTs() {
        KafkaJsonRecordConverter converter = converter(true);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":1,"
                                                + "\"isDdl\":false,"
                                                + MYSQL_TYPE
                                                + ",\"old\":null,\"pkNames\":[\"id\"],"
                                                + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                                                + "\"ts\":1598752887000,\"type\":\"INSERT\"}"),
                        "test.users",
                        0,
                        100L);
        SourceRecord record = records.get(0);
        // with EVENT_TIME=TS the offset event time follows `ts`
        assertEquals("1598752887000", record.sourceOffset().get("eventTime"));
        assertEquals(Long.valueOf(1598752887000L), record.timestamp());
    }

    @Test
    void testUpdateCarriesBeforeAndAfter() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\",\"name\":\"Bob\"}],"
                                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":2,"
                                                + "\"isDdl\":false,"
                                                + MYSQL_TYPE
                                                + ",\"old\":[{\"name\":\"Alice\"}],"
                                                + "\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                                                + "\"table\":\"users\",\"ts\":1598752887000,"
                                                + "\"type\":\"UPDATE\"}"),
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
    void testDeleteUsesDataAsBefore() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":3,"
                                                + "\"isDdl\":false,"
                                                + MYSQL_TYPE
                                                + ",\"old\":null,\"pkNames\":[\"id\"],"
                                                + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                                                + "\"ts\":1598752887000,\"type\":\"DELETE\"}"),
                        "test.users",
                        0,
                        100L);
        assertEquals(1, records.size());
        Struct value = (Struct) records.get(0).value();
        assertEquals("d", value.getString("op"));
        assertNull(value.getStruct("after"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
        // key resolved from the (before) data row
        assertEquals(1L, ((Struct) records.get(0).key()).getInt64("id"));
    }

    @Test
    void testDdlMessageProducesNoDataRecords() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":null,\"database\":\"test\","
                                                + "\"es\":1598752886000,\"id\":4,\"isDdl\":true,"
                                                + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                                                + "\"sql\":\"ALTER TABLE `users` ADD COLUMN `age` int\","
                                                + "\"sqlType\":null,\"table\":\"users\","
                                                + "\"ts\":1598752887000,\"type\":\"ALTER\"}"),
                        "test.users",
                        0,
                        100L);
        assertTrue(records.isEmpty());
    }

    @Test
    void testNonDmlMessageProducesNoDataRecords() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[],\"database\":\"test\","
                                                + "\"es\":1598752886000,\"id\":5,\"isDdl\":false,"
                                                + "\"mysqlType\":{},\"old\":null,\"pkNames\":null,"
                                                + "\"sql\":\"\",\"sqlType\":{},"
                                                + "\"table\":\"users\",\"ts\":1598752887000,"
                                                + "\"type\":\"GTID\"}"),
                        "test.users",
                        0,
                        100L);
        assertTrue(records.isEmpty());
    }

    @Test
    void testTidbWatermarkProducesNoDataRecords() {
        // TiCDC (with enable-tidb-extension=true) emits these marker events: isDdl=false but
        // type=TIDB_WATERMARK and data=null, so they must never be dispatched as DML rows.
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":null,\"database\":\"\",\"es\":1656559521880,"
                                                + "\"id\":0,\"isDdl\":false,\"mysqlType\":null,"
                                                + "\"old\":null,\"pkNames\":null,\"sql\":\"\","
                                                + "\"sqlType\":null,\"table\":\"\","
                                                + "\"ts\":1656559524120,\"type\":\"TIDB_WATERMARK\"}"),
                        "test.users",
                        0,
                        100L);
        assertTrue(records.isEmpty());
    }

    @Test
    void testTidbDeleteWithNullOldUsesDataAsBefore() {
        // TiCDC DELETE events put the deleted row in `data` and leave `old` null; the before image
        // and the key must come from `data`. Both es and ts are present and reliable (the es/ts
        // evidence P2-2 depends on).
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                                                + "\"database\":\"test\",\"es\":1656559521880,\"id\":3,"
                                                + "\"isDdl\":false,"
                                                + MYSQL_TYPE
                                                + ",\"old\":null,\"pkNames\":[\"id\"],"
                                                + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                                                + "\"ts\":1656559524120,\"type\":\"DELETE\"}"),
                        "test.users",
                        0,
                        100L);
        assertEquals(1, records.size());
        SourceRecord record = records.get(0);
        Struct value = (Struct) record.value();
        assertEquals("d", value.getString("op"));
        assertNull(value.getStruct("after"));
        assertEquals("Alice", value.getStruct("before").getString("name"));
        assertEquals(1L, ((Struct) record.key()).getInt64("id"));
        Struct source = value.getStruct("source");
        assertEquals(1656559521880L, source.getInt64("es"));
        assertEquals(1656559524120L, source.getInt64("ts"));
    }

    @Test
    void testSchemaIsCachedAcrossMessages() {
        KafkaJsonRecordConverter converter = converter(false);
        String insert =
                "{"
                        + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                        + "\"database\":\"test\",\"es\":1598752886000,\"id\":1,"
                        + "\"isDdl\":false,"
                        + MYSQL_TYPE
                        + ",\"old\":null,\"pkNames\":[\"id\"],"
                        + "\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                        + "\"ts\":1598752887000,\"type\":\"INSERT\"}";
        SourceRecord first =
                converter
                        .convert(new CanalMessageParser().parse(insert), "test.users", 0, 0L)
                        .get(0);
        SourceRecord second =
                converter
                        .convert(new CanalMessageParser().parse(insert), "test.users", 1, 1L)
                        .get(0);
        // the TableSchema (and hence the value schema) is registered once and reused
        assertEquals(first.valueSchema(), second.valueSchema());
        assertEquals(Integer.valueOf(1), second.kafkaPartition());
    }

    @Test
    void testSourcePartitionAlwaysServer() {
        KafkaJsonRecordConverter converter = converter(false);
        List<SourceRecord> records =
                converter.convert(
                        new CanalMessageParser()
                                .parse(
                                        "{"
                                                + "\"data\":[{\"id\":\"1\"}],"
                                                + "\"database\":\"test\",\"es\":1,\"id\":6,"
                                                + "\"isDdl\":false,"
                                                + "\"mysqlType\":{\"id\":\"bigint(20)\"},"
                                                + "\"old\":null,\"pkNames\":[\"id\"],"
                                                + "\"sql\":\"\",\"sqlType\":{\"id\":-5},"
                                                + "\"table\":\"users\",\"ts\":2,\"type\":\"INSERT\"}"),
                        "test.users",
                        3,
                        42L);
        Map<String, ?> partition = records.get(0).sourcePartition();
        assertEquals(1, partition.size());
        assertEquals("kafka_json_cdc_source", partition.get("server"));
    }
}
