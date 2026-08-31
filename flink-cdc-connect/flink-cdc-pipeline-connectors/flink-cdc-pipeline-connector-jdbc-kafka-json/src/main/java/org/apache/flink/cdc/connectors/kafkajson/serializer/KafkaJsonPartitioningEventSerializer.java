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

package org.apache.flink.cdc.connectors.kafkajson.serializer;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link PartitioningEvent}.
 *
 * <p>Mirrors the released {@code PartitioningEventSerializer} of flink-cdc-runtime, except that the
 * payload is serialized through the connector's {@link KafkaJsonEventSerializer}: the released one
 * delegates to the runtime {@code EventSerializer}, which cannot handle the connector's five custom
 * schema-change events. The wrapper itself (payload + {@code targetPartition}) is target-agnostic,
 * so reusing the released {@code PartitioningEvent} POJO is safe.
 */
public class KafkaJsonPartitioningEventSerializer
        extends TypeSerializerSingleton<PartitioningEvent> {

    public static final KafkaJsonPartitioningEventSerializer INSTANCE =
            new KafkaJsonPartitioningEventSerializer();

    private final KafkaJsonEventSerializer eventSerializer = KafkaJsonEventSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public PartitioningEvent createInstance() {
        return new PartitioningEvent(null, -1);
    }

    @Override
    public PartitioningEvent copy(PartitioningEvent from) {
        return new PartitioningEvent(
                eventSerializer.copy(from.getPayload()), from.getTargetPartition());
    }

    @Override
    public PartitioningEvent copy(PartitioningEvent from, PartitioningEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(PartitioningEvent record, DataOutputView target) throws IOException {
        eventSerializer.serialize(record.getPayload(), target);
        target.writeInt(record.getTargetPartition());
    }

    @Override
    public PartitioningEvent deserialize(DataInputView source) throws IOException {
        Event payload = eventSerializer.deserialize(source);
        int targetPartition = source.readInt();
        return new PartitioningEvent(payload, targetPartition);
    }

    @Override
    public PartitioningEvent deserialize(PartitioningEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        PartitioningEvent deserialized = deserialize(source);
        serialize(deserialized, target);
    }

    @Override
    public TypeSerializerSnapshot<PartitioningEvent> snapshotConfiguration() {
        return new KafkaJsonPartitioningEventSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link KafkaJsonPartitioningEventSerializer}. */
    public static final class KafkaJsonPartitioningEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<PartitioningEvent> {

        public KafkaJsonPartitioningEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
