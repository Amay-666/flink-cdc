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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;

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
    void testMissingEventTimeField() {
        // The extraction is unified on the message layer (KafkaJsonRecordConverter#eventTime):
        // es/ts are primitive long fields, so a message without them defaults to 0 rather than -1
        // (the -1 fallback is reserved for null/empty/unparsable input, see testInvalidJson).
        String message = "{\"data\":[]}";
        assertEquals(0L, KafkaJsonKafkaOffsetUtils.extractEventTime(message, EventTime.ES));
        assertEquals(0L, KafkaJsonKafkaOffsetUtils.extractEventTime(message, EventTime.TS));
    }

    @Test
    void testCanalTidbCommitTso() {
        // A DML message carries the commit TSO in `_tidb.commitTs`; >> 18 gives the physical millis.
        String dml =
                "{\"data\":[{\"id\":\"1\"}],\"database\":\"test\",\"es\":1598752886000,"
                        + "\"id\":1,\"isDdl\":false,\"mysqlType\":{},\"old\":null,"
                        + "\"pkNames\":null,\"sql\":\"\",\"sqlType\":{},\"table\":\"users\","
                        + "\"ts\":1598752887000,\"type\":\"INSERT\","
                        + "\"_tidb\":{\"commitTs\":4398046511104}}";
        assertEquals(
                16777216L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(dml, EventTime.TIDB_TSO));
    }

    @Test
    void testCanalTidbWatermarkTs() {
        // A watermark event carries no `_tidb.commitTs` but `_tidb.watermarkTs` (the TSO at which
        // every smaller commit TSO has been published); it yields a real event time in tidb_tso
        // mode, so the offset keeps advancing during quiet periods.
        String watermark =
                "{\"data\":null,\"database\":\"\",\"es\":1656559521880,"
                        + "\"id\":0,\"isDdl\":false,\"mysqlType\":null,"
                        + "\"old\":null,\"pkNames\":null,\"sql\":\"\","
                        + "\"sqlType\":null,\"table\":\"\","
                        + "\"ts\":1656559524120,\"type\":\"TIDB_WATERMARK\","
                        + "\"_tidb\":{\"watermarkTs\":4398046511104}}";
        assertEquals(16777216L, KafkaJsonKafkaOffsetUtils.extractEventTime(watermark, EventTime.TIDB_TSO));
        assertEquals(1656559521880L, KafkaJsonKafkaOffsetUtils.extractEventTime(watermark, EventTime.ES));
    }

    @Test
    void testCanalPlainMessageWithoutTidbInTsoMode() {
        // A plain canal flatMessage (no `_tidb`) has no TSO: tidb_tso yields null → -1.
        assertEquals(
                -1L, KafkaJsonKafkaOffsetUtils.extractEventTime(FLAT_MESSAGE, EventTime.TIDB_TSO));
    }

    @Test
    void testInvalidJson() {
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime("not-a-json", EventTime.ES));
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime(null, EventTime.TS));
        assertEquals(-1L, KafkaJsonKafkaOffsetUtils.extractEventTime("", EventTime.ES));
    }

    @Test
    void testDebeziumExtractSourceAndProcessingTime() {
        // ES -> payload.source.ts_ms (source change time), TS -> payload.ts_ms (processing time)
        String envelope =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[]},\"payload\":{"
                        + "\"before\":null,\"after\":{\"id\":1},"
                        + "\"source\":{\"connector\":\"mysql\",\"ts_ms\":1598752886000,"
                        + "\"db\":\"test\",\"table\":\"users\"},"
                        + "\"op\":\"c\",\"ts_ms\":1598752887000}}";
        assertEquals(
                1598752886000L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        envelope, EventTime.ES, MessageFormat.DEBEZIUM));
        assertEquals(
                1598752887000L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        envelope, EventTime.TS, MessageFormat.DEBEZIUM));
    }

    @Test
    void testDebeziumPayloadOnlyShape() {
        // schema-include=false: the payload fields sit at the top level
        String payloadOnly =
                "{\"after\":{\"id\":1},\"source\":{\"ts_ms\":100,\"db\":\"test\","
                        + "\"table\":\"users\"},\"op\":\"u\",\"ts_ms\":200}";
        assertEquals(
                100L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        payloadOnly, EventTime.ES, MessageFormat.DEBEZIUM));
        assertEquals(
                200L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        payloadOnly, EventTime.TS, MessageFormat.DEBEZIUM));
    }

    @Test
    void testDebeziumEsFallsBackToProcessingTime() {
        // a message without source.ts_ms (e.g. a bare schema-change record) uses payload.ts_ms
        String noSourceTime =
                "{\"payload\":{\"ddl\":\"ALTER TABLE `u` ADD COLUMN c INT\","
                        + "\"databaseName\":\"test\",\"tableChanges\":[],\"ts_ms\":200}}";
        assertEquals(
                200L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        noSourceTime, EventTime.ES, MessageFormat.DEBEZIUM));
    }

    @Test
    void testDebeziumTidbCommitTso() {
        long commitTs = 4398046511104L; // 16777216 ms << 18
        String ticdc =
                "{\"payload\":{\"after\":{\"id\":1},"
                        + "\"source\":{\"connector\":\"tidb\",\"commit_ts\":" + commitTs + ","
                        + "\"cluster_id\":\"c1\"},\"op\":\"c\",\"ts_ms\":200}}";
        assertEquals(
                16777216L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        ticdc, EventTime.TIDB_TSO, MessageFormat.DEBEZIUM));
    }

    @Test
    void testDebeziumTombstoneAndBlank() {
        assertEquals(
                -1L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        "{\"schema\":{\"type\":\"struct\"},\"payload\":null}",
                        EventTime.ES,
                        MessageFormat.DEBEZIUM));
        assertEquals(
                -1L,
                KafkaJsonKafkaOffsetUtils.extractEventTime(
                        "not-a-json", EventTime.ES, MessageFormat.DEBEZIUM));
    }
}
