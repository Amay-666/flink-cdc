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

package org.apache.flink.cdc.connectors.kafkajson.source.assigner.state;

import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsStateSerializer;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.split.KafkaJsonSplitSerializer;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the conditional versioning of {@link KafkaJsonPendingSplitsStateSerializer}
 * (FLINK-40697, connector-side port): the option off writes the base v6 format byte for byte; the
 * option on writes v7 (base + released flag inline); v7 states restore with the flag set.
 */
class KafkaJsonPendingSplitsStateSerializerTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    @Test
    void testOptionOffWritesBaseV6() throws IOException {
        KafkaJsonSplitSerializer splitSerializer =
                new KafkaJsonSplitSerializer(new KafkaJsonOffsetFactory());
        KafkaJsonPendingSplitsStateSerializer serializer =
                new KafkaJsonPendingSplitsStateSerializer(splitSerializer, false);
        PendingSplitsStateSerializer baseSerializer =
                new PendingSplitsStateSerializer(splitSerializer);

        assertEquals(6, serializer.getVersion());

        // byte-for-byte identical to the base serializer for the same state
        assertArrayEquals(
                baseSerializer.serialize(buildHybridState(false)),
                serializer.serialize(buildHybridState(false)));
    }

    @Test
    void testOptionOnWritesV7() throws IOException {
        KafkaJsonSplitSerializer splitSerializer =
                new KafkaJsonSplitSerializer(new KafkaJsonOffsetFactory());
        KafkaJsonPendingSplitsStateSerializer serializer =
                new KafkaJsonPendingSplitsStateSerializer(splitSerializer, true);

        assertEquals(7, serializer.getVersion());

        // a v7 state is larger than the v6 equivalent (one extra boolean)
        PendingSplitsStateSerializer baseSerializer =
                new PendingSplitsStateSerializer(splitSerializer);
        byte[] v6Bytes = baseSerializer.serialize(buildHybridState(false));
        byte[] v7Bytes = serializer.serialize(buildHybridState(false));
        assertEquals(v6Bytes.length + 1, v7Bytes.length);
    }

    @Test
    void testV7RoundTrip() throws IOException {
        KafkaJsonSplitSerializer splitSerializer =
                new KafkaJsonSplitSerializer(new KafkaJsonOffsetFactory());
        KafkaJsonPendingSplitsStateSerializer serializer =
                new KafkaJsonPendingSplitsStateSerializer(splitSerializer, true);

        // a released state round-trips: the flag survives, the state is our subclass
        PendingSplitsState released = buildHybridState(true);
        byte[] bytes = serializer.serialize(released);
        PendingSplitsState restored = serializer.deserialize(7, bytes);

        HybridPendingSplitsState hybrid =
                assertInstanceOf(HybridPendingSplitsState.class, restored);
        KafkaJsonSnapshotPendingSplitsState snapshot =
                assertInstanceOf(
                        KafkaJsonSnapshotPendingSplitsState.class,
                        hybrid.getSnapshotPendingSplits());
        assertTrue(snapshot.isSnapshotMetaReleased());
        assertEquals(0, snapshot.getAssignedSplits().size());
        assertEquals(0, snapshot.getSplitFinishedOffsets().size());
        assertEquals(1, snapshot.getAlreadyProcessedTables().size());

        // a non-released state round-trips without the flag
        PendingSplitsState notReleased = buildHybridState(false);
        byte[] notReleasedBytes = serializer.serialize(notReleased);
        PendingSplitsState restoredNotReleased = serializer.deserialize(7, notReleasedBytes);
        HybridPendingSplitsState hybridNotReleased =
                assertInstanceOf(HybridPendingSplitsState.class, restoredNotReleased);
        assertFalse(
                hybridNotReleased.getSnapshotPendingSplits()
                        instanceof KafkaJsonSnapshotPendingSplitsState);
    }

    @Test
    void testV6RestoresWithoutFlag() throws IOException {
        KafkaJsonSplitSerializer splitSerializer =
                new KafkaJsonSplitSerializer(new KafkaJsonOffsetFactory());
        KafkaJsonPendingSplitsStateSerializer serializer =
                new KafkaJsonPendingSplitsStateSerializer(splitSerializer, true);

        // a v6 checkpoint (written by a default-off job or an older build) restores without
        // the flag: the returned state is a base class (not our subclass)
        PendingSplitsStateSerializer baseSerializer =
                new PendingSplitsStateSerializer(splitSerializer);
        byte[] v6Bytes = baseSerializer.serialize(buildHybridState(false));
        PendingSplitsState restored = serializer.deserialize(6, v6Bytes);

        HybridPendingSplitsState hybrid =
                assertInstanceOf(HybridPendingSplitsState.class, restored);
        assertFalse(
                hybrid.getSnapshotPendingSplits() instanceof KafkaJsonSnapshotPendingSplitsState);
    }

    private static PendingSplitsState buildHybridState(boolean released) {
        return new HybridPendingSplitsState(
                new KafkaJsonSnapshotPendingSplitsState(
                        Collections.singletonList(TABLE_ID),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        Collections.emptyList(),
                        true,
                        true,
                        released),
                true);
    }
}
