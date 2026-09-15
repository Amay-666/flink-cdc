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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent.TableRename;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TypeSerializer} for {@link RenameTableEvent}.
 *
 * <p>Not part of the released flink-cdc-runtime serialization stack: it serializes the raw DDL and
 * every {@code old -> new} pair of the statement (each with the schema of its renamed table), and
 * is wired in only via the connector's own {@link KafkaJsonEventSerializer} / {@link
 * KafkaJsonSchemaChangeEventSerializer}.
 *
 * <p>The format has no version marker of its own, so an event written by the single-pair revision
 * of this serializer cannot be read back — a job restored from such a checkpoint must start over
 * (the source replays its Kafka offsets, so nothing is lost beyond the re-read).
 */
public class KafkaJsonRenameTableEventSerializer extends TypeSerializerSingleton<RenameTableEvent> {

    private static final long serialVersionUID = 2L;

    /** Sharable instance of the KafkaJsonRenameTableEventSerializer. */
    public static final KafkaJsonRenameTableEventSerializer INSTANCE =
            new KafkaJsonRenameTableEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public RenameTableEvent createInstance() {
        return new RenameTableEvent(
                TableId.tableId("unknown"),
                TableId.tableId("unknown"),
                Schema.newBuilder().build());
    }

    @Override
    public RenameTableEvent copy(RenameTableEvent from) {
        List<TableRename> pairs = new ArrayList<>(from.getPairs().size());
        for (TableRename pair : from.getPairs()) {
            pairs.add(
                    new TableRename(
                            tableIdSerializer.copy(pair.getOldTableId()),
                            tableIdSerializer.copy(pair.getNewTableId()),
                            schemaSerializer.copy(pair.getSchema())));
        }
        return new RenameTableEvent(pairs, from.getSql());
    }

    @Override
    public RenameTableEvent copy(RenameTableEvent from, RenameTableEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RenameTableEvent record, DataOutputView target) throws IOException {
        stringSerializer.serialize(record.getSql(), target);
        List<TableRename> pairs = record.getPairs();
        IntSerializer.INSTANCE.serialize(pairs.size(), target);
        for (TableRename pair : pairs) {
            tableIdSerializer.serialize(pair.getOldTableId(), target);
            tableIdSerializer.serialize(pair.getNewTableId(), target);
            schemaSerializer.serialize(pair.getSchema(), target);
        }
    }

    @Override
    public RenameTableEvent deserialize(DataInputView source) throws IOException {
        String sql = stringSerializer.deserialize(source);
        int pairCount = IntSerializer.INSTANCE.deserialize(source);
        List<TableRename> pairs = new ArrayList<>(pairCount);
        for (int i = 0; i < pairCount; i++) {
            pairs.add(
                    new TableRename(
                            tableIdSerializer.deserialize(source),
                            tableIdSerializer.deserialize(source),
                            schemaSerializer.deserialize(source)));
        }
        return new RenameTableEvent(pairs, sql);
    }

    @Override
    public RenameTableEvent deserialize(RenameTableEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<RenameTableEvent> snapshotConfiguration() {
        return new KafkaJsonRenameTableEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class KafkaJsonRenameTableEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RenameTableEvent> {

        public KafkaJsonRenameTableEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
