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

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage.MessageType;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessageParser;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for {@link DebeziumMessageParser}. */
class DebeziumMessageParserTest {

    private static final String STANDARD_DML =
            "{"
                    + "\"schema\":{\"type\":\"struct\",\"fields\":[]},"
                    + "\"payload\":{"
                    + "\"before\":null,"
                    + "\"after\":{\"id\":1,\"name\":\"Alice\"},"
                    + "\"source\":{\"version\":\"1.9.8.Final\",\"connector\":\"mysql\","
                    + "\"name\":\"server\",\"ts_ms\":1598752886000,\"db\":\"test\","
                    + "\"table\":\"users\"},"
                    + "\"op\":\"c\",\"ts_ms\":1598752887000,\"transaction\":null"
                    + "}"
                    + "}";

    private static final DebeziumMessageParser PARSER = new DebeziumMessageParser();

    @Test
    void testStandardEnvelopeDml() {
        DebeziumMessage message = PARSER.parse(STANDARD_DML);
        assertEquals(MessageType.DML, message.getMessageType());
        assertEquals("test", message.getDatabase());
        assertEquals("users", message.getTable());
        JsonNode after = message.getPayload().getAfter();
        assertEquals(1, after.get("id").asInt());
        assertEquals("Alice", after.get("name").asText());
        assertEquals("c", message.getPayload().getOp());
    }

    @Test
    void testSchemaIncludedButNoSchemaField() {
        // the payload wrapper without the schema field binds the same way
        DebeziumMessage message =
                PARSER.parse(
                        "{\"payload\":{\"after\":{\"id\":1},\"source\":{\"db\":\"test\","
                                + "\"table\":\"users\",\"ts_ms\":100},\"op\":\"c\",\"ts_ms\":200}}");
        assertEquals(MessageType.DML, message.getMessageType());
        assertEquals("test", message.getDatabase());
        assertEquals("users", message.getTable());
    }

    @Test
    void testPayloadOnlyNoWrapper() {
        // schema-include=false: the payload fields sit at the top level
        DebeziumMessage message =
                PARSER.parse(
                        "{\"after\":{\"id\":1},\"source\":{\"db\":\"test\","
                                + "\"table\":\"users\",\"ts_ms\":100},\"op\":\"u\",\"ts_ms\":200}");
        assertEquals(MessageType.DML, message.getMessageType());
        assertEquals("test", message.getDatabase());
        assertEquals("users", message.getTable());
        assertEquals("u", message.getPayload().getOp());
    }

    @Test
    void testDeleteMessage() {
        DebeziumMessage message =
                PARSER.parse(
                        "{\"payload\":{\"before\":{\"id\":1},\"after\":null,"
                                + "\"source\":{\"db\":\"test\",\"table\":\"users\"},"
                                + "\"op\":\"d\",\"ts_ms\":200}}");
        assertEquals(MessageType.DML, message.getMessageType());
        assertEquals(1, message.getPayload().getBefore().get("id").asInt());
        assertNull(message.getPayload().getAfter());
    }

    @Test
    void testTidbWatermark() {
        DebeziumMessage message =
                PARSER.parse(
                        "{\"payload\":{\"before\":null,\"after\":null,"
                                + "\"source\":{\"connector\":\"tidb\",\"commit_ts\":4398046511104,"
                                + "\"cluster_id\":\"c1\"},\"op\":\"m\",\"ts_ms\":200}}");
        assertEquals(MessageType.TIDB_WATERMARK, message.getMessageType());
    }

    @Test
    void testTidbDmlWithCommitTs() {
        // commit_ts is a TSO; physical millis = commit_ts >> 18
        long commitTs = 4398046511104L; // 16777216 ms << 18
        DebeziumMessage message =
                PARSER.parse(
                        "{\"payload\":{\"after\":{\"id\":1},"
                                + "\"source\":{\"connector\":\"tidb\",\"db\":\"test\","
                                + "\"table\":\"users\",\"commit_ts\":"
                                + commitTs
                                + ",\"cluster_id\":\"c1\"},"
                                + "\"op\":\"c\",\"ts_ms\":200}}");
        assertEquals(MessageType.DML, message.getMessageType());
        assertEquals(16777216L, message.getEventTimeValue(EventTime.TIDB_TSO));
    }

    @Test
    void testSchemaChangeRecord() {
        // the bare schema-change record shape (schema-history topic / include.schema.changes)
        DebeziumMessage message =
                PARSER.parse(
                        "{\"databaseName\":\"test\","
                                + "\"ddl\":\"ALTER TABLE `users` ADD COLUMN `age` INT\","
                                + "\"tableChanges\":[]}");
        assertEquals(MessageType.DDL, message.getMessageType());
        assertEquals("test", message.getDatabase());
        assertNull(message.getTable());
        assertEquals("ALTER TABLE `users` ADD COLUMN `age` INT", message.getSql());
    }

    @Test
    void testEventTimeValues() {
        DebeziumMessage message = PARSER.parse(STANDARD_DML);
        // ES maps to source.ts_ms (source change time), TS to payload.ts_ms (processing time)
        assertEquals(1598752886000L, message.getEventTimeValue(EventTime.ES));
        assertEquals(1598752887000L, message.getEventTimeValue(EventTime.TS));
        // no commit_ts in a standard Debezium message
        assertNull(message.getEventTimeValue(EventTime.TIDB_TSO));
    }

    @Test
    void testEmptyDbTableNormalizedToNull() {
        DebeziumMessage message =
                PARSER.parse(
                        "{\"payload\":{\"source\":{\"db\":\"\",\"table\":null},"
                                + "\"op\":\"c\",\"ts_ms\":200}}");
        assertNull(message.getDatabase());
        assertNull(message.getTable());
    }

    @Test
    void testTombstoneNullPayload() {
        DebeziumMessage message =
                PARSER.parse("{\"schema\":{\"type\":\"struct\",\"fields\":[]},\"payload\":null}");
        assertEquals(MessageType.UNKNOWN, message.getMessageType());
        assertNull(message.getPayload());
        assertNull(message.getDatabase());
    }

    @Test
    void testNullAndBlankJson() {
        assertNull(PARSER.parse(null));
        assertNull(PARSER.parse(""));
        assertNull(PARSER.parse("   "));
    }

    @Test
    void testInvalidJsonThrows() {
        assertThrows(IllegalArgumentException.class, () -> PARSER.parse("not-a-json"));
    }

    @Test
    void testFormat() {
        assertEquals(
                org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions
                        .MessageFormat.DEBEZIUM,
                PARSER.getFormat());
    }
}
