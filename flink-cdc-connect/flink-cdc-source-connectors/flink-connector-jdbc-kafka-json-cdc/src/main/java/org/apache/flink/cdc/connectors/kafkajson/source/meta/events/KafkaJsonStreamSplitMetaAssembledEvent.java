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

package org.apache.flink.cdc.connectors.kafkajson.source.meta.events;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.enumerator.KafkaJsonIncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.kafkajson.source.reader.KafkaJsonSourceReader;

/**
 * The {@link SourceEvent} that {@link KafkaJsonSourceReader} sends to {@link
 * KafkaJsonIncrementalSourceEnumerator} once it has assembled the complete stream-split metadata
 * and no longer needs the coordinator to serve that metadata, so the coordinator can release it
 * from memory and from the checkpointed state once the assembled split is covered by a completed
 * checkpoint. Connector-side port of the base framework's FLINK-40697 mechanism (which itself
 * generalizes the MySQL release, FLINK-39775).
 */
public class KafkaJsonStreamSplitMetaAssembledEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /**
     * Sentinel generation reported when the stream split was small enough to ship its metadata
     * inline, so the reader never requested meta groups and never learned an assignment generation.
     * The coordinator always accepts this value, which is safe because an inline reader needs no
     * metadata from the coordinator.
     */
    public static final int COMPLETE_WITHOUT_META_GENERATION = -1;

    private final String splitId;
    private final int totalFinishedSplitSize;

    /**
     * The metadata assignment generation under which the reader assembled the split, echoed from
     * the meta group events, or {@link #COMPLETE_WITHOUT_META_GENERATION} for an inline split. The
     * coordinator releases only if this still matches its current generation, so a stale report
     * from a re-assigned (failed-over) stream reader cannot trigger a premature release.
     */
    private final int assignmentGeneration;

    public KafkaJsonStreamSplitMetaAssembledEvent(
            String splitId, int totalFinishedSplitSize, int assignmentGeneration) {
        this.splitId = splitId;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.assignmentGeneration = assignmentGeneration;
    }

    public String getSplitId() {
        return splitId;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public int getAssignmentGeneration() {
        return assignmentGeneration;
    }
}
