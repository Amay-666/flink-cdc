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

package org.apache.flink.cdc.connectors.kafkajson.unit.event;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.BatchCreateTableEvents;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonBatchCreateTableEventsSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link BatchCreateTableEvents#partition}, the split the source uses to turn the
 * cached schemas into records.
 *
 * <p>Both limits matter and neither alone is enough: the table count bounds how many DDLs one
 * schema-change round applies against the external system, the byte count bounds the encoded record
 * — so a chunk must respect whichever binds first, without dropping, duplicating or reordering a
 * single table. A table larger than the whole byte budget has to travel alone instead of stalling
 * the split.
 */
public class BatchCreateTableEventsTest {

    @Test
    public void testSplitsByTableCount() throws Exception {
        List<CreateTableEvent> creates = creates(250, 0);

        List<BatchCreateTableEvents> batches =
                BatchCreateTableEvents.partition(creates, 100, 1024 * 1024);

        // Small schemas: the byte budget never binds, so only the table count does.
        assertThat(batches).hasSize(3);
        assertThat(tableCounts(batches)).containsExactly(100, 100, 50);
        assertThat(flatten(batches)).containsExactlyElementsOf(creates);
        assertSizesWithin(batches, 1024 * 1024);
    }

    @Test
    public void testSplitsByEncodedSize() throws Exception {
        // Each table carries ~28KB of comments, so the 64KB budget admits two tables per batch
        // while
        // the table count of 100 would admit all five.
        List<CreateTableEvent> creates = creates(5, 7_000);
        assertThat(serializedSize(new BatchCreateTableEvents(creates))).isGreaterThan(64 * 1024);

        List<BatchCreateTableEvents> batches =
                BatchCreateTableEvents.partition(creates, 100, 64 * 1024);

        // Bytes bind, not the table count: the chunks are 2, 2, 1 and none exceeds the budget.
        assertThat(tableCounts(batches)).containsExactly(2, 2, 1);
        assertSizesWithin(batches, 64 * 1024);
        assertThat(flatten(batches)).containsExactlyElementsOf(creates);
    }

    @Test
    public void testTableLargerThanByteBudgetTravelsAlone() throws Exception {
        List<CreateTableEvent> creates = creates(3, 200_000);
        BatchCreateTableEvents oversized = new BatchCreateTableEvents(creates.subList(0, 1));
        assertThat(serializedSize(oversized)).isGreaterThan(64 * 1024);

        List<BatchCreateTableEvents> batches =
                BatchCreateTableEvents.partition(creates, 100, 64 * 1024);

        // The split must make progress: an oversized table becomes a table-count-1 batch rather
        // than an empty batch or an unbounded one.
        assertThat(tableCounts(batches)).containsExactly(1, 1, 1);
        assertThat(flatten(batches)).containsExactlyElementsOf(creates);
    }

    @Test
    public void testEmptyInputProducesNoBatch() {
        assertThat(BatchCreateTableEvents.partition(new ArrayList<>(), 100, 1024 * 1024)).isEmpty();
    }

    /** Creates {@code count} tables, each with comments of {@code commentLength} characters. */
    private static List<CreateTableEvent> creates(int count, int commentLength) {
        List<CreateTableEvent> creates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            creates.add(
                    new CreateTableEvent(
                            TableId.tableId("test", "t" + i), schema(commentLength, i)));
        }
        return creates;
    }

    private static Schema schema(int commentLength, int seed) {
        Schema.Builder builder = Schema.newBuilder().physicalColumn("id", DataTypes.INT());
        for (int column = 0; column < 4; column++) {
            builder.physicalColumn(
                    "c" + seed + "_" + column, DataTypes.STRING(), repeat('c', commentLength));
        }
        return builder.build();
    }

    private static String repeat(char c, int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private static List<Integer> tableCounts(List<BatchCreateTableEvents> batches) {
        List<Integer> counts = new ArrayList<>(batches.size());
        for (BatchCreateTableEvents batch : batches) {
            counts.add(batch.getCreateTableEvents().size());
        }
        return counts;
    }

    private static List<CreateTableEvent> flatten(List<BatchCreateTableEvents> batches) {
        List<CreateTableEvent> creates = new ArrayList<>();
        for (BatchCreateTableEvents batch : batches) {
            creates.addAll(batch.getCreateTableEvents());
        }
        return creates;
    }

    /** Asserts the encoded size of every batch, i.e. the size of the record it becomes. */
    private static void assertSizesWithin(List<BatchCreateTableEvents> batches, int maxBytes)
            throws Exception {
        for (BatchCreateTableEvents batch : batches) {
            assertThat(serializedSize(batch)).isLessThanOrEqualTo(maxBytes);
        }
    }

    private static int serializedSize(BatchCreateTableEvents batch) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        KafkaJsonBatchCreateTableEventsSerializer.INSTANCE.serialize(
                batch, new DataOutputViewStreamWrapper(out));
        return out.size();
    }
}
