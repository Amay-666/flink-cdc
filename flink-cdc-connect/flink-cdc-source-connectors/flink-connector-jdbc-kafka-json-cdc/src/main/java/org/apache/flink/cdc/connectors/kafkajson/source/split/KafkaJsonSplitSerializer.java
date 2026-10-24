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

package org.apache.flink.cdc.connectors.kafkajson.source.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.List;

/**
 * The split serializer of the jdbc-kafka-json source, a drop-in replacement for the anonymous
 * {@link SourceSplitSerializer} that {@code IncrementalSource} builds by default.
 *
 * <p>Why a copy of the base's serialize logic: the base's {@code writeFinishedSplitsInfo}
 * delegates to {@code FinishedSnapshotSplitInfo#serialize(out)}, whose trailing {@code
 * getCopyOfBuffer()} returns a copy of the <b>whole accumulated buffer</b> per finished split
 * (FLINK-38184). The stream split of a billion-row source carries ~120k finished split infos,
 * so one checkpoint paid gigabytes of discarded copies — minutes of blocked reading. The loop
 * is inlined here writing the very same fields (byte-identical format, see the base's {@code
 * readFinishedSplitsInfo}) without the per-entry copies; MySqlSplitSerializer of the MySQL
 * connector is the precedent for a connector owning its split serializer.
 *
 * <p>Byte-compatibility contract: {@link #serialize(SourceSplitBase)} produces exactly the
 * bytes the base's serializer produces for the same split (same version, same field order), so
 * checkpoints written by either serializer deserialize through the inherited read path — old
 * checkpoints restore unchanged, and mixed versions during a rolling restart are safe.
 *
 * <p>The base's {@code serializedFormCache} optimization (a package-private field on the split
 * classes) cannot be used from this package; skipping it only costs re-serializing the current
 * in-flight split per checkpoint — a few hundred bytes for a snapshot split, the linear size of
 * the stream split, which is the unavoidable state size anyway.
 */
public class KafkaJsonSplitSerializer extends SourceSplitSerializer {

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int STREAM_SPLIT_FLAG = 2;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

    private final OffsetFactory offsetFactory;

    public KafkaJsonSplitSerializer(OffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public OffsetFactory getOffsetFactory() {
        return offsetFactory;
    }

    @Override
    public byte[] serialize(SourceSplitBase split) throws IOException {
        if (split.isSnapshotSplit()) {
            final SnapshotSplit snapshotSplit = split.asSnapshotSplit();
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(SNAPSHOT_SPLIT_FLAG);
            boolean useCatalogBeforeSchema =
                    SerializerUtils.shouldUseCatalogBeforeSchema(snapshotSplit.getTableId());
            out.writeBoolean(useCatalogBeforeSchema);
            out.writeUTF(snapshotSplit.getTableId().toDoubleQuotedString());
            out.writeUTF(snapshotSplit.splitId());
            out.writeUTF(snapshotSplit.getSplitKeyType().asSerializableString());

            final Object[] splitStart = snapshotSplit.getSplitStart();
            final Object[] splitEnd = snapshotSplit.getSplitEnd();
            // rowToSerializedString deals null case
            out.writeUTF(SerializerUtils.rowToSerializedString(splitStart));
            out.writeUTF(SerializerUtils.rowToSerializedString(splitEnd));
            writeOffsetPosition(snapshotSplit.getHighWatermark(), out);
            writeTableSchemas(snapshotSplit.getTableSchemas(), out);
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            return result;
        } else {
            final StreamSplit streamSplit = split.asStreamSplit();
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(STREAM_SPLIT_FLAG);
            out.writeUTF(streamSplit.splitId());
            out.writeUTF("");
            writeOffsetPosition(streamSplit.getStartingOffset(), out);
            writeOffsetPosition(streamSplit.getEndingOffset(), out);
            writeFinishedSplitsInfo(streamSplit.getFinishedSnapshotSplitInfos(), out);
            writeTableSchemas(streamSplit.getTableSchemas(), out);
            out.writeInt(streamSplit.getTotalFinishedSplitSize());
            out.writeBoolean(streamSplit.isSuspended());
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            return result;
        }
    }

    /**
     * Writes the finished split infos of the stream split with the field order of the base's
     * {@code readFinishedSplitsInfo}: tableId, splitId, splitStart, splitEnd, offsetFactory,
     * highWatermark, useCatalogBeforeSchema. The base's version delegates to {@code
     * FinishedSnapshotSplitInfo#serialize(out)} and pays a full-buffer copy per entry (the
     * discarded return value); the writes here are byte-identical but copy-free.
     */
    private void writeFinishedSplitsInfo(
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo) {
            out.writeUTF(splitInfo.getTableId().toString());
            out.writeUTF(splitInfo.getSplitId());
            out.writeUTF(SerializerUtils.rowToSerializedString(splitInfo.getSplitStart()));
            out.writeUTF(SerializerUtils.rowToSerializedString(splitInfo.getSplitEnd()));
            out.writeUTF(SerializerUtils.rowToSerializedString(splitInfo.getOffsetFactory()));
            writeOffsetPosition(splitInfo.getHighWatermark(), out);
            out.writeBoolean(
                    SerializerUtils.shouldUseCatalogBeforeSchema(splitInfo.getTableId()));
        }
    }
}
