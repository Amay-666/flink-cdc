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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonFlatMessageParser}. */
class KafkaJsonFlatMessageParserTest {

    @Test
    void testParseInsert() {
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{"
                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":1,"
                                + "\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\","
                                + "\"name\":\"varchar(255)\"},\"old\":null,"
                                + "\"pkNames\":[\"id\"],\"sql\":\"\","
                                + "\"sqlType\":{\"id\":-5,\"name\":12},"
                                + "\"table\":\"users\",\"ts\":1598752887000,\"type\":\"INSERT\"}");
        assertEquals(1, message.getId());
        assertEquals("test", message.getDatabase());
        assertEquals("users", message.getTable());
        assertFalse(message.isDdl());
        assertEquals("INSERT", message.getType());
        assertEquals(1598752886000L, message.getEs());
        assertEquals(1598752887000L, message.getTs());
        assertEquals(Arrays.asList("id"), message.getPkNames());
        assertEquals("bigint(20)", message.getMysqlType().get("id"));
        assertEquals(Integer.valueOf(-5), message.getSqlType().get("id"));
        assertEquals(1, message.getData().size());
        assertEquals("Alice", message.getData().get(0).get("name"));
        assertTrue(message.getOld().isEmpty());
    }

    @Test
    void testParseUpdateWithOld() {
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{"
                                + "\"data\":[{\"id\":\"1\",\"name\":\"Bob\"}],"
                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":2,"
                                + "\"isDdl\":false,\"mysqlType\":{},\"old\":[{\"name\":\"Alice\"}],"
                                + "\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                                + "\"table\":\"users\",\"ts\":1598752887000,\"type\":\"UPDATE\"}");
        assertEquals("UPDATE", message.getType());
        assertEquals("Bob", message.getData().get(0).get("name"));
        assertEquals("Alice", message.getOld().get(0).get("name"));
    }

    @Test
    void testParseDelete() {
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{"
                                + "\"data\":[{\"id\":\"1\",\"name\":\"Alice\"}],"
                                + "\"database\":\"test\",\"es\":1598752886000,\"id\":3,"
                                + "\"isDdl\":false,\"mysqlType\":{},\"old\":null,"
                                + "\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                                + "\"table\":\"users\",\"ts\":1598752887000,\"type\":\"DELETE\"}");
        assertEquals("DELETE", message.getType());
        assertEquals(1, message.getData().size());
    }

    @Test
    void testParseDdl() {
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{"
                                + "\"data\":null,\"database\":\"test\",\"es\":1598752886000,"
                                + "\"id\":4,\"isDdl\":true,\"mysqlType\":null,\"old\":null,"
                                + "\"pkNames\":null,"
                                + "\"sql\":\"ALTER TABLE `users` ADD COLUMN `age` int\","
                                + "\"sqlType\":null,\"table\":\"users\","
                                + "\"ts\":1598752887000,\"type\":\"ALTER\"}");
        assertTrue(message.isDdl());
        assertEquals("ALTER", message.getType());
        assertEquals("ALTER TABLE `users` ADD COLUMN `age` int", message.getSql());
        assertTrue(message.getData().isEmpty());
        assertTrue(message.getMysqlType().isEmpty());
    }

    @Test
    void testMissingFieldsAreTolerated() {
        // A minimal message missing most fields must not fail parsing
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse("{\"data\":[{\"id\":\"1\"}]}");
        assertEquals(0, message.getId());
        assertNull(message.getDatabase());
        assertNull(message.getTable());
        assertFalse(message.isDdl());
        assertEquals(1, message.getData().size());
        assertTrue(message.getOld().isEmpty());
        assertTrue(message.getMysqlType().isEmpty());
        assertTrue(message.getSqlType().isEmpty());
        assertEquals("1", message.getData().get(0).get("id"));
    }

    @Test
    void testEmptyDataWithTypeAndTable() {
        // canal emits such messages for GTID / QUERY / TRUNCATE events
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{\"data\":[],\"database\":\"test\",\"es\":1598752886000,"
                                + "\"id\":5,\"isDdl\":false,\"mysqlType\":{},\"old\":null,"
                                + "\"pkNames\":null,\"sql\":\"\",\"sqlType\":{},"
                                + "\"table\":\"users\",\"ts\":1598752887000,\"type\":\"GTID\"}");
        assertTrue(message.getData().isEmpty());
        assertEquals("GTID", message.getType());
    }

    @Test
    void testNullAndEmptyJson() {
        assertNull(KafkaJsonFlatMessageParser.parse(null));
        assertNull(KafkaJsonFlatMessageParser.parse(""));
        assertNull(KafkaJsonFlatMessageParser.parse("  "));
    }

    @Test
    void testInvalidJsonThrows() {
        assertThrows(
                org.apache.flink.util.FlinkRuntimeException.class,
                () -> KafkaJsonFlatMessageParser.parse("not-a-json"));
    }

    @Test
    void testMultiRowBatch() {
        // one DML statement affecting multiple rows is delivered as one message
        Map<String, String> row1 = new HashMap<>();
        row1.put("id", "1");
        Map<String, String> row2 = new HashMap<>();
        row2.put("id", "2");
        KafkaJsonFlatMessage message =
                KafkaJsonFlatMessageParser.parse(
                        "{"
                                + "\"data\":[{\"id\":\"1\"},{\"id\":\"2\"}],"
                                + "\"database\":\"test\",\"es\":1,\"id\":6,"
                                + "\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\"},"
                                + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\","
                                + "\"sqlType\":{\"id\":-5},\"table\":\"users\","
                                + "\"ts\":2,\"type\":\"INSERT\"}");
        assertEquals(2, message.getData().size());
        assertEquals("2", message.getData().get(1).get("id"));
    }
}
