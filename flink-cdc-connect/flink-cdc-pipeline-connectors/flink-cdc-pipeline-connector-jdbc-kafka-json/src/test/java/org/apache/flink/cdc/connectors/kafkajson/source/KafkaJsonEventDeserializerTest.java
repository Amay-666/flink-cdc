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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the epoch-timestamp conversion of {@link KafkaJsonEventDeserializer}: a value
 * before the epoch must split into a negative millisecond and a non-negative
 * nano-of-millisecond, which is what {@link TimestampData}'s contract requires. The shared
 * parent truncates toward zero and fails its checkArgument on pre-1970 values.
 */
class KafkaJsonEventDeserializerTest {

    private final KafkaJsonEventDeserializer deserializer =
            new KafkaJsonEventDeserializer(DebeziumChangelogMode.ALL, false);

    @Test
    void convertsPreEpochMicroTimestamp() {
        // -1500 micros = 1.5 millis BEFORE the epoch: floor division splits it into
        // -2 millis + 500_000 nanos; the parent's truncation produced -500_000 nanos
        // and failed TimestampData's checkArgument
        TimestampData timestampData =
                (TimestampData)
                        deserializer.convertToTimestamp(-1500L, MicroTimestamp.builder().build());
        assertThat(timestampData.getMillisecond()).isEqualTo(-2L);
        assertThat(timestampData.getNanoOfMillisecond()).isEqualTo(500_000);
    }

    @Test
    void convertsPostEpochMicroTimestamp() {
        // positive values are unchanged by the floor division: 1500 micros = 1 ms + 500_000 ns
        TimestampData timestampData =
                (TimestampData)
                        deserializer.convertToTimestamp(1500L, MicroTimestamp.builder().build());
        assertThat(timestampData.getMillisecond()).isEqualTo(1L);
        assertThat(timestampData.getNanoOfMillisecond()).isEqualTo(500_000);
    }

    @Test
    void convertsPreEpochNanoTimestamp() {
        // -1_500_500_000 nanos = 1500.5 millis before the epoch: -1501 millis + 500_000 nanos
        TimestampData timestampData =
                (TimestampData)
                        deserializer.convertToTimestamp(
                                -1_500_500_000L, NanoTimestamp.builder().build());
        assertThat(timestampData.getMillisecond()).isEqualTo(-1501L);
        assertThat(timestampData.getNanoOfMillisecond()).isEqualTo(500_000);
    }

    @Test
    void convertsPreEpochMilliTimestampThroughTheParent() {
        // the millisecond schema delegates to the parent: the value passes through unchanged
        // (a negative millisecond is legal, the nanos stay zero)
        TimestampData timestampData =
                (TimestampData)
                        deserializer.convertToTimestamp(
                                -123L, io.debezium.time.Timestamp.builder().build());
        assertThat(timestampData.getMillisecond()).isEqualTo(-123L);
        assertThat(timestampData.getNanoOfMillisecond()).isEqualTo(0);
    }
}
