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

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for the event time extraction of {@link KafkaJsonKafkaOffsetUtils}. */
class KafkaJsonKafkaOffsetUtilsTest {

    private static final String FLAT_MESSAGE =
            "{\"data\":[],\"database\":\"test\",\"es\":1598752886000,"
                    + "\"id\":1,\"isDdl\":false,\"mysqlType\":{},\"old\":null,"
                    + "\"pkNames\":null,\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                    + "\"ts\":1598752887000,\"type\":\"INSERT\"}";

    @Test
    void testExtractExecuteTime() {
        assertEquals(
                1598752886000L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(FLAT_MESSAGE, EventTime.ES));
    }

    @Test
    void testExtractSendTime() {
        assertEquals(
                1598752887000L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(FLAT_MESSAGE, EventTime.TS));
    }

    @Test
    void testMissingField() {
        String message = "{\"data\":[]}";
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime(message, EventTime.ES));
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime(message, EventTime.TS));
    }

    @Test
    void testInvalidJson() {
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime("not-a-json", EventTime.ES));
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime(null, EventTime.TS));
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime("", EventTime.ES));
    }
}
