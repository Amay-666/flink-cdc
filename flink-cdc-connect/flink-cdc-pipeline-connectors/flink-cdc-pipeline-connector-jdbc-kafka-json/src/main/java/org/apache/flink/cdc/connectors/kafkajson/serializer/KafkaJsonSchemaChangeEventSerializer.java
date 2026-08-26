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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.runtime.serializer.EnumSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.event.AddColumnEventSerializer;
import org.apache.flink.cdc.runtime.serializer.event.AlterColumnTypeEventSerializer;
import org.apache.flink.cdc.runtime.serializer.event.CreateTableEventSerializer;
import org.apache.flink.cdc.runtime.serializer.event.DropColumnEventSerializer;
import org.apache.flink.cdc.runtime.serializer.event.RenameColumnEventSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link SchemaChangeEvent} that additionally handles {@link
 * RenameTableEvent} and {@link TruncateTableEvent}.
 *
 * <p>This is the canal-connector-local copy of the released {@code SchemaChangeEventSerializer}, with
 * the discriminator replaced by the connector's own {@link KafkaJsonSchemaChangeTag} so that a {@link
 * RenameTableEvent} or {@link TruncateTableEvent} — which the released {@code SchemaChangeEventType}
 * enum has no values for — can be serialized alongside the five released event types. The five known
 * events reuse the released per-event serializers and their byte formats; only the {@code RENAME_TABLE}
 * and {@code TRUNCATE_TABLE} tags are new.
 */
public final class KafkaJsonSchemaChangeEventSerializer
        extends TypeSerializerSingleton<SchemaChangeEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the KafkaJsonSchemaChangeEventSerializer. */
    public static final KafkaJsonSchemaChangeEventSerializer INSTANCE =
            new KafkaJsonSchemaChangeEventSerializer();

    private final EnumSerializer<KafkaJsonSchemaChangeTag> enumSerializer =
            new EnumSerializer<>(KafkaJsonSchemaChangeTag.class);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public SchemaChangeEvent createInstance() {
        return new SchemaChangeEvent() {
            @Override
            public TableId tableId() {
                return TableId.tableId("unknown", "unknown", "unknown");
            }

            @Override
            public org.apache.flink.cdc.common.event.SchemaChangeEventType getType() {
                return null;
            }
        };
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from) {
        if (from instanceof AlterColumnTypeEvent) {
            return AlterColumnTypeEventSerializer.INSTANCE.copy((AlterColumnTypeEvent) from);
        } else if (from instanceof CreateTableEvent) {
            return CreateTableEventSerializer.INSTANCE.copy((CreateTableEvent) from);
        } else if (from instanceof RenameColumnEvent) {
            return RenameColumnEventSerializer.INSTANCE.copy((RenameColumnEvent) from);
        } else if (from instanceof AddColumnEvent) {
            return AddColumnEventSerializer.INSTANCE.copy((AddColumnEvent) from);
        } else if (from instanceof DropColumnEvent) {
            return DropColumnEventSerializer.INSTANCE.copy((DropColumnEvent) from);
        } else if (from instanceof RenameTableEvent) {
            return KafkaJsonRenameTableEventSerializer.INSTANCE.copy((RenameTableEvent) from);
        } else if (from instanceof TruncateTableEvent) {
            return KafkaJsonTruncateTableEventSerializer.INSTANCE.copy((TruncateTableEvent) from);
        } else {
            throw new IllegalArgumentException("Unknown schema change event: " + from);
        }
    }

    @Override
    public SchemaChangeEvent copy(SchemaChangeEvent from, SchemaChangeEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(SchemaChangeEvent record, DataOutputView target) throws IOException {
        if (record instanceof AlterColumnTypeEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.ALTER_COLUMN_TYPE, target);
            AlterColumnTypeEventSerializer.INSTANCE.serialize(
                    (AlterColumnTypeEvent) record, target);
        } else if (record instanceof CreateTableEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.CREATE_TABLE, target);
            CreateTableEventSerializer.INSTANCE.serialize((CreateTableEvent) record, target);
        } else if (record instanceof RenameColumnEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.RENAME_COLUMN, target);
            RenameColumnEventSerializer.INSTANCE.serialize((RenameColumnEvent) record, target);
        } else if (record instanceof AddColumnEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.ADD_COLUMN, target);
            AddColumnEventSerializer.INSTANCE.serialize((AddColumnEvent) record, target);
        } else if (record instanceof DropColumnEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.DROP_COLUMN, target);
            DropColumnEventSerializer.INSTANCE.serialize((DropColumnEvent) record, target);
        } else if (record instanceof RenameTableEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.RENAME_TABLE, target);
            KafkaJsonRenameTableEventSerializer.INSTANCE.serialize(
                    (RenameTableEvent) record, target);
        } else if (record instanceof TruncateTableEvent) {
            enumSerializer.serialize(KafkaJsonSchemaChangeTag.TRUNCATE_TABLE, target);
            KafkaJsonTruncateTableEventSerializer.INSTANCE.serialize(
                    (TruncateTableEvent) record, target);
        } else {
            throw new IllegalArgumentException("Unknown schema change event: " + record);
        }
    }

    @Override
    public SchemaChangeEvent deserialize(DataInputView source) throws IOException {
        KafkaJsonSchemaChangeTag tag = enumSerializer.deserialize(source);
        switch (tag) {
            case ADD_COLUMN:
                return AddColumnEventSerializer.INSTANCE.deserialize(source);
            case DROP_COLUMN:
                return DropColumnEventSerializer.INSTANCE.deserialize(source);
            case CREATE_TABLE:
                return CreateTableEventSerializer.INSTANCE.deserialize(source);
            case RENAME_COLUMN:
                return RenameColumnEventSerializer.INSTANCE.deserialize(source);
            case ALTER_COLUMN_TYPE:
                return AlterColumnTypeEventSerializer.INSTANCE.deserialize(source);
            case RENAME_TABLE:
                return KafkaJsonRenameTableEventSerializer.INSTANCE.deserialize(source);
            case TRUNCATE_TABLE:
                return KafkaJsonTruncateTableEventSerializer.INSTANCE.deserialize(source);
            default:
                throw new IllegalArgumentException(
                        "Unknown schema change event class: " + tag);
        }
    }

    @Override
    public SchemaChangeEvent deserialize(SchemaChangeEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<SchemaChangeEvent> snapshotConfiguration() {
        return new KafkaJsonSchemaChangeEventSerializerSnapshot();
    }

    /**
     * Serializer configuration snapshot for compatibility and format evolution.
     */
    @SuppressWarnings("WeakerAccess")
    public static final class KafkaJsonSchemaChangeEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<SchemaChangeEvent> {

        public KafkaJsonSchemaChangeEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }

    /**
     * The per-event discriminator written before each serialized schema change.
     */
    enum KafkaJsonSchemaChangeTag {
        ADD_COLUMN,
        DROP_COLUMN,
        CREATE_TABLE,
        RENAME_COLUMN,
        ALTER_COLUMN_TYPE,
        RENAME_TABLE,
        TRUNCATE_TABLE
    }
}
