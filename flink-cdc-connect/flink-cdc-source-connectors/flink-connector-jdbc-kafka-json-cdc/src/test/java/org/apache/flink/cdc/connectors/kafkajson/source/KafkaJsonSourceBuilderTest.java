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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the {@link KafkaJsonSourceBuilder} wiring: the {@link KafkaJsonSource} created by {@code
 * build()} is driven entirely by flink-cdc-base (no custom reader/enumerator), so this test locks
 * the contract the base framework relies on - boundedness, produced type and the split serializer
 * whose offset (de)serialization goes through the wired {@code KafkaJsonOffsetFactory}.
 */
class KafkaJsonSourceBuilderTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");
    private static final RowType SPLIT_KEY_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("id", new BigIntType())));

    @Test
    void testBuildReturnsWiredSource() {
        KafkaJsonSource<String> source = initialSource();
        assertNotNull(source);
        assertTrue(source.getSplitSerializer() != null);
        assertTrue(source.getEnumeratorCheckpointSerializer() != null);
    }

    @Test
    void testBoundednessOfInitialIsContinuousUnbounded() {
        // the default startup mode (initial) runs snapshot then incremental => unbounded
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, initialSource().getBoundedness());
    }

    @Test
    void testBoundednessOfSnapshotIsBounded() {
        KafkaJsonSource<String> source =
                KafkaJsonSourceBuilder.<String>builder()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .startupOptions(StartupOptions.snapshot())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void testProducedTypeComesFromDeserializer() {
        JsonDebeziumDeserializationSchema deserializer = new JsonDebeziumDeserializationSchema();
        KafkaJsonSource<String> source =
                KafkaJsonSourceBuilder.<String>builder()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .deserializer(deserializer)
                        .build();
        assertEquals(deserializer.getProducedType(), source.getProducedType());
    }

    @Test
    void testSplitSerializerRoundTripsStreamSplitWithKafkaJsonOffsets() throws Exception {
        KafkaJsonSource<String> source = initialSource();
        KafkaJsonOffset startingOffset = new KafkaJsonOffset(1000L, 0, 5);
        KafkaJsonOffset endingOffset = new KafkaJsonOffset(2000L, 1, 9);
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0);

        StreamSplit restored = (StreamSplit) roundTrip(source, split);

        assertEquals(split.splitId(), restored.splitId());
        assertEquals(startingOffset, restored.getStartingOffset());
        assertEquals(endingOffset, restored.getEndingOffset());
    }

    @Test
    void testSplitSerializerRoundTripsSnapshotSplitWithHighWatermark() throws Exception {
        KafkaJsonSource<String> source = initialSource();
        SnapshotSplit split =
                new SnapshotSplit(
                        TABLE_ID,
                        "users-split-0",
                        SPLIT_KEY_TYPE,
                        new Object[] {1L},
                        new Object[] {3L},
                        new KafkaJsonOffset(1000L, 0, 5),
                        Collections.singletonMap(
                                TABLE_ID,
                                new TableChange(TableChangeType.CREATE, table())));

        SnapshotSplit restored = (SnapshotSplit) roundTrip(source, split);

        assertEquals(TABLE_ID, restored.getTableId());
        assertEquals(split.splitId(), restored.splitId());
        assertArrayEquals(new Object[] {1L}, restored.getSplitStart());
        assertArrayEquals(new Object[] {3L}, restored.getSplitEnd());
        assertEquals(new KafkaJsonOffset(1000L, 0, 5), restored.getHighWatermark());
    }

    private static KafkaJsonSource<String> initialSource() {
        return KafkaJsonSourceBuilder.<String>builder()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
    }

    private static SourceSplitBase roundTrip(KafkaJsonSource<String> source, SourceSplitBase split)
            throws Exception {
        SimpleVersionedSerializer<SourceSplitBase> serializer = source.getSplitSerializer();
        byte[] bytes = serializer.serialize(split);
        return serializer.deserialize(serializer.getVersion(), bytes);
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
}
