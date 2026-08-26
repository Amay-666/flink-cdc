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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link TruncateTableEvent}.
 *
 * <p>Not part of the released flink-cdc-runtime serialization stack: it serializes the table id,
 * the schema of the truncated table and the raw DDL, and is wired in only via the canal connector's
 * own {@link KafkaJsonEventSerializer} / {@link KafkaJsonSchemaChangeEventSerializer}.
 */
public class KafkaJsonTruncateTableEventSerializer
        extends TypeSerializerSingleton<TruncateTableEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the KafkaJsonTruncateTableEventSerializer. */
    public static final KafkaJsonTruncateTableEventSerializer INSTANCE =
            new KafkaJsonTruncateTableEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TruncateTableEvent createInstance() {
        return new TruncateTableEvent(TableId.tableId("unknown"), Schema.newBuilder().build());
    }

    @Override
    public TruncateTableEvent copy(TruncateTableEvent from) {
        return new TruncateTableEvent(
                tableIdSerializer.copy(from.tableId()),
                schemaSerializer.copy(from.getSchema()),
                from.getSql());
    }

    @Override
    public TruncateTableEvent copy(TruncateTableEvent from, TruncateTableEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(TruncateTableEvent record, DataOutputView target) throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        schemaSerializer.serialize(record.getSchema(), target);
        stringSerializer.serialize(record.getSql(), target);
    }

    @Override
    public TruncateTableEvent deserialize(DataInputView source) throws IOException {
        return new TruncateTableEvent(
                tableIdSerializer.deserialize(source),
                schemaSerializer.deserialize(source),
                stringSerializer.deserialize(source));
    }

    @Override
    public TruncateTableEvent deserialize(TruncateTableEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<TruncateTableEvent> snapshotConfiguration() {
        return new KafkaJsonTruncateTableEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class KafkaJsonTruncateTableEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<TruncateTableEvent> {

        public KafkaJsonTruncateTableEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
