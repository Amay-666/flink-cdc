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

package org.apache.flink.cdc.connectors.kafkajson.source.assigner;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.kafkajson.source.assigner.state.KafkaJsonSnapshotPendingSplitsState;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the snapshot-metadata release of {@link KafkaJsonSnapshotSplitAssigner}
 * (FLINK-40697, connector-side port): the release clears the bulk maps, keeps the survival set
 * ({@code alreadyProcessedTables}, status), and the flag round-trips through the assigner state.
 */
class KafkaJsonSnapshotSplitAssignerReleaseTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    @Test
    void testReleaseClearsBulkMapsAndSetsFlag() {
        KafkaJsonSnapshotPendingSplitsState state = buildSnapshotState();
        KafkaJsonSnapshotSplitAssigner<SourceConfig> assigner =
                new KafkaJsonSnapshotSplitAssigner<>(null, 1, state, null, null);

        assertFalse(assigner.isSnapshotMetaReleased());
        assertEquals(2, assigner.getAssignedSplits().size());
        assertEquals(2, assigner.getSplitFinishedOffsets().size());
        assertEquals(1, assigner.getTableSchemas().size());

        assigner.releaseSnapshotMetadata();

        assertTrue(assigner.isSnapshotMetaReleased());
        assertEquals(0, assigner.getAssignedSplits().size());
        assertEquals(0, assigner.getSplitFinishedOffsets().size());
        assertEquals(0, assigner.getTableSchemas().size());

        // the survival set stays (checked via the checkpointed state)
        PendingSplitsState after = assigner.snapshotState(1L);
        KafkaJsonSnapshotPendingSplitsState released = (KafkaJsonSnapshotPendingSplitsState) after;
        assertEquals(1, released.getAlreadyProcessedTables().size());
    }

    @Test
    void testReleasedFlagRoundTripsThroughCheckpointState() {
        KafkaJsonSnapshotPendingSplitsState state = buildSnapshotState();
        KafkaJsonSnapshotSplitAssigner<SourceConfig> assigner =
                new KafkaJsonSnapshotSplitAssigner<>(null, 1, state, null, null);

        // snapshot before release: the flag is false
        PendingSplitsState beforeRelease = assigner.snapshotState(1L);
        assertFalse(((KafkaJsonSnapshotPendingSplitsState) beforeRelease).isSnapshotMetaReleased());

        assigner.releaseSnapshotMetadata();

        // snapshot after release: the flag is true and the bulk maps are empty in the state
        PendingSplitsState afterRelease = assigner.snapshotState(2L);
        KafkaJsonSnapshotPendingSplitsState released =
                (KafkaJsonSnapshotPendingSplitsState) afterRelease;
        assertTrue(released.isSnapshotMetaReleased());
        assertEquals(0, released.getAssignedSplits().size());
        assertEquals(0, released.getSplitFinishedOffsets().size());
        assertEquals(0, released.getTableSchemas().size());
        assertEquals(1, released.getAlreadyProcessedTables().size());
    }

    @Test
    void testRestoredAssignerKeepsReleaseInEffect() {
        KafkaJsonSnapshotPendingSplitsState releasedState =
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
                        true);
        KafkaJsonSnapshotSplitAssigner<SourceConfig> assigner =
                new KafkaJsonSnapshotSplitAssigner<>(null, 1, releasedState, null, null);

        assertTrue(assigner.isSnapshotMetaReleased());
        assertEquals(0, assigner.getAssignedSplits().size());
        // a second release is a no-op (the flag is already set)
        assigner.releaseSnapshotMetadata();
        assertTrue(assigner.isSnapshotMetaReleased());
        // the survival set is intact
        PendingSplitsState after = assigner.snapshotState(1L);
        assertEquals(
                1,
                ((KafkaJsonSnapshotPendingSplitsState) after).getAlreadyProcessedTables().size());
    }

    private static KafkaJsonSnapshotPendingSplitsState buildSnapshotState() {
        return new KafkaJsonSnapshotPendingSplitsState(
                Collections.singletonList(TABLE_ID),
                Collections.emptyList(),
                buildAssignedSplits(),
                buildTableSchemas(),
                buildFinishedOffsets(),
                AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                Collections.emptyList(),
                true,
                true,
                false);
    }

    private static Map<
                    String,
                    org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit>
            buildAssignedSplits() {
        Map<String, org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit>
                splits = new HashMap<>();
        splits.put("split-0", buildSplit("split-0"));
        splits.put("split-1", buildSplit("split-1"));
        return splits;
    }

    private static org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit
            buildSplit(String splitId) {
        return new org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit(
                TABLE_ID, splitId, null, null, null, null);
    }

    private static Map<TableId, TableChanges.TableChange> buildTableSchemas() {
        Map<TableId, TableChanges.TableChange> schemas = new HashMap<>();
        schemas.put(
                TABLE_ID,
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, buildTable()));
        return schemas;
    }

    private static io.debezium.relational.Table buildTable() {
        return io.debezium.relational.Table.editor()
                .tableId(TABLE_ID)
                .addColumn(
                        io.debezium.relational.Column.editor()
                                .name("id")
                                .type("BIGINT")
                                .jdbcType(java.sql.Types.BIGINT)
                                .length(20)
                                .optional(false)
                                .position(1)
                                .create())
                .addColumn(
                        io.debezium.relational.Column.editor()
                                .name("name")
                                .type("VARCHAR")
                                .jdbcType(java.sql.Types.VARCHAR)
                                .length(255)
                                .optional(true)
                                .position(2)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static Map<String, org.apache.flink.cdc.connectors.base.source.meta.offset.Offset>
            buildFinishedOffsets() {
        Map<String, org.apache.flink.cdc.connectors.base.source.meta.offset.Offset> offsets =
                new HashMap<>();
        offsets.put("split-0", null);
        offsets.put("split-1", null);
        return offsets;
    }
}
