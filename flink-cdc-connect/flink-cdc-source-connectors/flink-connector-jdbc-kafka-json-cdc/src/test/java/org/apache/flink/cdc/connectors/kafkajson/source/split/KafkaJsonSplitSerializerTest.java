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
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonSource;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The byte-format contract of {@link KafkaJsonSplitSerializer}: its output must be identical to
 * the serializer the base framework would build, so checkpoints of either serializer restore
 * through the shared read path. The custom write path exists only to drop the quadratic
 * full-buffer copies the base pays per finished split info (FLINK-38184).
 */
class KafkaJsonSplitSerializerTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");
    private static final RowType SPLIT_KEY_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("id", new BigIntType())));

    private final KafkaJsonSplitSerializer serializer =
            new KafkaJsonSplitSerializer(new KafkaJsonOffsetFactory());

    @Test
    void testSnapshotSplitBytesMatchTheBaseSerializer() throws Exception {
        SnapshotSplit split = snapshotSplit();

        assertArrayEquals(baseSerializer().serialize(split), serializer.serialize(split));
    }

    @Test
    void testStreamSplitBytesMatchTheBaseSerializer() throws Exception {
        StreamSplit split = streamSplit(500);

        assertArrayEquals(baseSerializer().serialize(split), serializer.serialize(split));
    }

    @Test
    void testStreamSplitRoundTripsThroughTheInheritedReadPath() throws Exception {
        StreamSplit split = streamSplit(200);

        byte[] bytes = serializer.serialize(split);
        assertEquals(split, serializer.deserialize(serializer.getVersion(), bytes));
    }

    @Test
    void testSnapshotSplitRoundTripsThroughTheInheritedReadPath() throws Exception {
        SnapshotSplit split = snapshotSplit();

        byte[] bytes = serializer.serialize(split);
        assertEquals(split, serializer.deserialize(serializer.getVersion(), bytes));
    }

    @Test
    void testBaseSerializerBytesRestoreThroughTheCustomSerializer() throws Exception {
        // checkpoints written by the base serializer (before the switch, or by a mixed-version
        // job during a rolling restart) must restore through this serializer unchanged
        StreamSplit split = streamSplit(100);

        byte[] baseBytes = baseSerializer().serialize(split);
        assertEquals(split, serializer.deserialize(serializer.getVersion(), baseBytes));
    }

    @Test
    void testSourceExposesTheCopyFreeSerializer() {
        KafkaJsonSourceConfigFactory factory = configFactory();

        KafkaJsonSource<Object> source =
                new KafkaJsonSource<>(
                        factory,
                        null,
                        new KafkaJsonOffsetFactory(),
                        new KafkaJsonDialect(factory.create(0)));

        // the split-assignment RPC and the enumerator state serializer pick it up through
        // getSplitSerializer; the reader receives it in createReader
        assertTrue(source.getSplitSerializer() instanceof KafkaJsonSplitSerializer);
    }

    /**
     * The serializer {@code IncrementalSource} would build by default — the reference for the
     * byte-format contract.
     */
    private static SourceSplitSerializer baseSerializer() {
        return new SourceSplitSerializer() {
            @Override
            public OffsetFactory getOffsetFactory() {
                return new KafkaJsonOffsetFactory();
            }
        };
    }

    private static SnapshotSplit snapshotSplit() {
        return new SnapshotSplit(
                TABLE_ID,
                "snapshot-split-0",
                SPLIT_KEY_TYPE,
                new Object[] {1L},
                new Object[] {1000L},
                new KafkaJsonOffset(2000, 0, 5),
                tableSchemas());
    }

    private static StreamSplit streamSplit(int finishedSplits) {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        for (int i = 0; i < finishedSplits; i++) {
            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            TABLE_ID,
                            "snapshot-split-" + i,
                            new Object[] {(long) (i * 1000)},
                            new Object[] {(long) (i * 1000 + 999)},
                            new KafkaJsonOffset(1000 + i, 0, i),
                            new KafkaJsonOffsetFactory()));
        }
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID,
                new KafkaJsonOffset(2000, 0, 5),
                KafkaJsonOffset.NO_STOPPING_OFFSET,
                finishedSplitsInfo,
                tableSchemas(),
                finishedSplitsInfo.size(),
                false);
    }

    private static Map<TableId, TableChange> tableSchemas() {
        return new HashMap<>(
                Collections.singletonMap(
                        TABLE_ID, new TableChange(TableChangeType.CREATE, table())));
    }

    private static Table table() {
        return Table.editor()
                .tableId(TABLE_ID)
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("BIGINT")
                                .jdbcType(Types.BIGINT)
                                .length(20)
                                .optional(false)
                                .position(1)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("name")
                                .type("VARCHAR")
                                .jdbcType(Types.VARCHAR)
                                .length(255)
                                .optional(true)
                                .position(2)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static KafkaJsonSourceConfigFactory configFactory() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t");
    }
}
