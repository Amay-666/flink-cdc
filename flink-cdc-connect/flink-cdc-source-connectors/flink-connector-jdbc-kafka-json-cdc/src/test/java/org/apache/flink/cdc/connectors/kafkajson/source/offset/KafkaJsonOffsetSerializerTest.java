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

package org.apache.flink.cdc.connectors.kafkajson.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that a {@link KafkaJsonOffset} survives the checkpoint serialization round-trip through the
 * base {@link SourceSplitSerializer}. This is the exact code path used when a {@link StreamSplit}
 * (whose starting offset is a {@link KafkaJsonOffset}) is checkpointed and restored.
 */
class KafkaJsonOffsetSerializerTest {

    @Test
    void testStreamSplitRoundTripPreservesKafkaJsonOffset() throws IOException {
        KafkaJsonOffsetFactory offsetFactory = new KafkaJsonOffsetFactory();
        SourceSplitSerializer serializer =
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                };

        KafkaJsonOffset startingOffset = new KafkaJsonOffset(1598752886000L, 0, 42L);
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        startingOffset,
                        KafkaJsonOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0);

        byte[] serialized = serializer.serialize(split);
        SourceSplitBase restored = serializer.deserialize(serializer.getVersion(), serialized);

        StreamSplit restoredSplit = restored.asStreamSplit();
        assertEquals(startingOffset, restoredSplit.getStartingOffset());
        assertEquals(KafkaJsonOffset.NO_STOPPING_OFFSET, restoredSplit.getEndingOffset());

        KafkaJsonOffset restoredStartingOffset = (KafkaJsonOffset) restoredSplit.getStartingOffset();
        assertEquals(1598752886000L, restoredStartingOffset.getEventTime());
        assertEquals(0, restoredStartingOffset.getPartition());
        assertEquals(42L, restoredStartingOffset.getOffsetValue());
    }

    @Test
    void testRoundTripWithInitialOffset() throws IOException {
        KafkaJsonOffsetFactory offsetFactory = new KafkaJsonOffsetFactory();
        SourceSplitSerializer serializer =
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                };

        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        KafkaJsonOffset.INITIAL_OFFSET,
                        KafkaJsonOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0);

        byte[] serialized = serializer.serialize(split);
        SourceSplitBase restored = serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(KafkaJsonOffset.INITIAL_OFFSET, restored.asStreamSplit().getStartingOffset());
        assertEquals(
                KafkaJsonOffset.NO_STOPPING_OFFSET, restored.asStreamSplit().getEndingOffset());
    }
}
