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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for the {@link KafkaJsonMessage} abstraction as implemented by {@code
 * CanalMessage}. */
class KafkaJsonMessageTest {

    private static final String BASE =
            "{\"database\":\"test\",\"table\":\"users\",\"es\":1000,\"ts\":2000,"
                    + "\"data\":[{\"id\":\"1\"}],\"isDdl\":false,\"type\":\"%s\"}";

    private static CanalMessage message(String type, boolean isDdl) {
        return new CanalMessageParser().parse(
                BASE.replace("%s", type).replace("\"isDdl\":false", "\"isDdl\":" + isDdl));
    }

    @Test
    void testDmlTypes() {
        assertEquals(MessageType.DML, message("INSERT", false).getMessageType());
        assertEquals(MessageType.DML, message("UPDATE", false).getMessageType());
        assertEquals(MessageType.DML, message("DELETE", false).getMessageType());
        assertEquals(MessageType.DML, message("QUERY", false).getMessageType());
    }

    @Test
    void testDdlType() {
        assertEquals(MessageType.DDL, message("ALTER", true).getMessageType());
        // type wins over isDdl; any message flagged as DDL is a DDL
        assertEquals(MessageType.DDL, message("INSERT", true).getMessageType());
    }

    @Test
    void testTidbWatermarkType() {
        assertEquals(
                MessageType.TIDB_WATERMARK, message("TIDB_WATERMARK", false).getMessageType());
    }

    @Test
    void testUnknownType() {
        assertEquals(MessageType.UNKNOWN, message("GTID", false).getMessageType());
        // a message without a type field is UNKNOWN too
        CanalMessage noType =
                new CanalMessageParser().parse(
                        "{\"database\":\"test\",\"table\":\"users\",\"es\":1000,"
                                + "\"data\":[{\"id\":\"1\"}],\"isDdl\":false}");
        assertEquals(MessageType.UNKNOWN, noType.getMessageType());
    }

    @Test
    void testEventTimeValues() {
        CanalMessage message = message("INSERT", false);
        assertEquals(1000L, message.getEventTimeValue(EventTime.ES));
        assertEquals(2000L, message.getEventTimeValue(EventTime.TS));
        // a canal flatMessage carries no `_tidb`; TIDB_TSO yields null (no usable TSO)
        assertNull(message.getEventTimeValue(EventTime.TIDB_TSO));
    }
}
