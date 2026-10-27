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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.event.BatchCreateTableEvents;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.event.CreateTableEventSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link TypeSerializer} for {@link BatchCreateTableEvents}.
 *
 * <p>Writes the number of tables followed by each table's {@link CreateTableEvent}, in emission
 * order, reusing the released {@code CreateTableEventSerializer} so a single create keeps its
 * released byte format. Wired in only via the connector's own {@link
 * KafkaJsonSchemaChangeEventSerializer}.
 */
public class KafkaJsonBatchCreateTableEventsSerializer
        extends TypeSerializerSingleton<BatchCreateTableEvents> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the KafkaJsonBatchCreateTableEventsSerializer. */
    public static final KafkaJsonBatchCreateTableEventsSerializer INSTANCE =
            new KafkaJsonBatchCreateTableEventsSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public BatchCreateTableEvents createInstance() {
        return new BatchCreateTableEvents(
                Collections.singletonList(
                        new CreateTableEvent(
                                TableId.tableId("unknown"), Schema.newBuilder().build())));
    }

    @Override
    public BatchCreateTableEvents copy(BatchCreateTableEvents from) {
        List<CreateTableEvent> creates = new ArrayList<>(from.getCreateTableEvents().size());
        for (CreateTableEvent create : from.getCreateTableEvents()) {
            creates.add(CreateTableEventSerializer.INSTANCE.copy(create));
        }
        return new BatchCreateTableEvents(creates);
    }

    @Override
    public BatchCreateTableEvents copy(BatchCreateTableEvents from, BatchCreateTableEvents reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BatchCreateTableEvents record, DataOutputView target) throws IOException {
        List<CreateTableEvent> creates = record.getCreateTableEvents();
        IntSerializer.INSTANCE.serialize(creates.size(), target);
        for (CreateTableEvent create : creates) {
            CreateTableEventSerializer.INSTANCE.serialize(create, target);
        }
    }

    @Override
    public BatchCreateTableEvents deserialize(DataInputView source) throws IOException {
        int tableCount = IntSerializer.INSTANCE.deserialize(source);
        List<CreateTableEvent> creates = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++) {
            creates.add(CreateTableEventSerializer.INSTANCE.deserialize(source));
        }
        return new BatchCreateTableEvents(creates);
    }

    @Override
    public BatchCreateTableEvents deserialize(BatchCreateTableEvents reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<BatchCreateTableEvents> snapshotConfiguration() {
        return new KafkaJsonBatchCreateTableEventsSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class KafkaJsonBatchCreateTableEventsSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<BatchCreateTableEvents> {

        public KafkaJsonBatchCreateTableEventsSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
