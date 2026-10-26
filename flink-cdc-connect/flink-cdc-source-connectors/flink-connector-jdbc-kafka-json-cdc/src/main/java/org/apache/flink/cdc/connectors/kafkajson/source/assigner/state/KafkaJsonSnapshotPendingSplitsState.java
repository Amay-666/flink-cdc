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
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The jdbc-kafka-json snapshot assigner state: the base {@link SnapshotPendingSplitsState} plus the
 * {@code snapshotMetaReleased} flag of FLINK-40697. The flag travels only in version 7 (written
 * only when {@code scan.incremental.snapshot.metadata.release.enabled} is set, see {@code
 * KafkaJsonPendingSplitsStateSerializer}); an older format restores with the flag defaulting to
 * {@code false}, and a v7 state restores as released with the bulk maps empty.
 */
public class KafkaJsonSnapshotPendingSplitsState extends SnapshotPendingSplitsState {

    /** Whether the bulk snapshot split metadata has been released (FLINK-40697). */
    private final boolean snapshotMetaReleased;

    public KafkaJsonSnapshotPendingSplitsState(
            List<TableId> alreadyProcessedTables,
            List<SchemalessSnapshotSplit> remainingSplits,
            Map<String, SchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            Map<String, Offset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            boolean snapshotMetaReleased) {
        super(
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                tableSchemas,
                splitFinishedOffsets,
                assignerStatus,
                remainingTables,
                isTableIdCaseSensitive,
                isRemainingTablesCheckpointed);
        this.snapshotMetaReleased = snapshotMetaReleased;
    }

    public boolean isSnapshotMetaReleased() {
        return snapshotMetaReleased;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaJsonSnapshotPendingSplitsState)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return snapshotMetaReleased
                == ((KafkaJsonSnapshotPendingSplitsState) o).snapshotMetaReleased;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(snapshotMetaReleased);
    }

    @Override
    public String toString() {
        // strip the base prefix and closing brace, then append the flag
        final String base = super.toString();
        return "KafkaJsonSnapshotPendingSplitsState{"
                + base.substring(
                        "SnapshotPendingSplitsState{".length(),
                        Math.max(base.length() - 1, "SnapshotPendingSplitsState{".length()))
                + ", snapshotMetaReleased="
                + snapshotMetaReleased
                + '}';
    }
}
