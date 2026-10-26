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

import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The kafkajson meta-group event: the base {@link StreamSplitMetaEvent} plus the {@code
 * assignmentGeneration} of FLINK-40697. The reader echoes this generation back in the assembled
 * report ({@link KafkaJsonStreamSplitMetaAssembledEvent}) so the coordinator can reject a stale
 * report from a failed-over reader.
 *
 * <p>The base reader handles this event transparently: the {@code instanceof StreamSplitMetaEvent}
 * check passes, and the base fields (splitId, metaGroupId, metaGroup, totalFinishedSplitSize) drive
 * the metadata-assembly loop. Only the kafkajson reader extension reads the generation field.
 */
public class KafkaJsonStreamSplitMetaEvent extends StreamSplitMetaEvent {

    private static final long serialVersionUID = 1L;

    /**
     * Generation of the stream-split assignment these meta groups belong to. The reader echoes it
     * back in the assembled report so the coordinator can reject a stale report from a failed
     * reader attempt.
     */
    private final int assignmentGeneration;

    public KafkaJsonStreamSplitMetaEvent(
            String splitId,
            int metaGroupId,
            @Nullable List<byte[]> metaGroup,
            int totalFinishedSplitSize,
            int assignmentGeneration) {
        super(splitId, metaGroupId, metaGroup, totalFinishedSplitSize);
        this.assignmentGeneration = assignmentGeneration;
    }

    public int getAssignmentGeneration() {
        return assignmentGeneration;
    }
}
