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

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link KafkaJsonOffset} and its {@link KafkaJsonOffsetFactory}. The offset is a
 * non-standard 3-tuple {@code (eventTime, partition, offsetValue)}: it identifies a Kafka position
 * by the event time of the last message, so the ordering here is the contract the split assigner
 * relies on.
 */
class KafkaJsonOffsetTest {

    private final KafkaJsonOffsetFactory factory = new KafkaJsonOffsetFactory();

    @Test
    void testCompareToLexicographicOrder() {
        KafkaJsonOffset a = new KafkaJsonOffset(100L, 0, 5L);
        KafkaJsonOffset b = new KafkaJsonOffset(100L, 0, 6L);
        KafkaJsonOffset c = new KafkaJsonOffset(100L, 1, 0L);
        KafkaJsonOffset d = new KafkaJsonOffset(101L, 0, 0L);

        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertTrue(b.compareTo(c) < 0);
        assertTrue(c.compareTo(d) < 0);
        assertEquals(
                0, new KafkaJsonOffset(100L, 0, 5L).compareTo(new KafkaJsonOffset(100L, 0, 5L)));
    }

    @Test
    void testIsBeforeAfter() {
        KafkaJsonOffset low = new KafkaJsonOffset(100L, 0, 5L);
        KafkaJsonOffset high = new KafkaJsonOffset(200L, 3, 10L);

        assertTrue(low.isBefore(high));
        assertTrue(high.isAfter(low));
        assertTrue(low.isAtOrBefore(high));
        assertTrue(low.isAtOrBefore(new KafkaJsonOffset(100L, 0, 5L)));
        assertTrue(high.isAtOrAfter(low));
    }

    @Test
    void testInitialAndNoStopping() {
        assertTrue(KafkaJsonOffset.INITIAL_OFFSET.isBefore(KafkaJsonOffset.NO_STOPPING_OFFSET));
        assertTrue(new KafkaJsonOffset(0L, 0, 0L).isAfter(KafkaJsonOffset.INITIAL_OFFSET));
        assertTrue(new KafkaJsonOffset(0L, 0, 0L).isBefore(KafkaJsonOffset.NO_STOPPING_OFFSET));
        assertTrue(
                KafkaJsonOffset.NO_STOPPING_OFFSET.isAfter(
                        new KafkaJsonOffset(Long.MAX_VALUE - 1, 999, 999L)));
    }

    @Test
    void testEquals() {
        assertEquals(new KafkaJsonOffset(1L, 2, 3L), new KafkaJsonOffset(1L, 2, 3L));
        assertNotEquals(new KafkaJsonOffset(1L, 2, 3L), new KafkaJsonOffset(1L, 2, 4L));
        assertNotEquals(new KafkaJsonOffset(1L, 2, 3L), new KafkaJsonOffset(1L, 3, 3L));
    }

    @Test
    void testOfMap() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put("eventTime", "100");
        offsetMap.put("partition", "2");
        offsetMap.put("offset", "7");

        KafkaJsonOffset offset = KafkaJsonOffset.of(offsetMap);
        assertEquals(100L, offset.getEventTime());
        assertEquals(2, offset.getPartition());
        assertEquals(7L, offset.getOffsetValue());
    }

    @Test
    void testGettersAndOffsetMap() {
        KafkaJsonOffset offset = new KafkaJsonOffset(100L, 2, 7L);
        assertEquals(100L, offset.getEventTime());
        assertEquals(2, offset.getPartition());
        assertEquals(7L, offset.getOffsetValue());
        assertEquals("100", offset.getOffset().get("eventTime"));
        assertEquals("2", offset.getOffset().get("partition"));
        assertEquals("7", offset.getOffset().get("offset"));
    }

    @Test
    void testFactoryNewOffsetFromMap() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put("eventTime", "123");
        offsetMap.put("partition", "3");
        offsetMap.put("offset", "9");

        Offset offset = factory.newOffset(offsetMap);
        assertTrue(offset instanceof KafkaJsonOffset);
        KafkaJsonOffset canalOffset = (KafkaJsonOffset) offset;
        assertEquals(123L, canalOffset.getEventTime());
        assertEquals(3, canalOffset.getPartition());
        assertEquals(9L, canalOffset.getOffsetValue());
    }

    @Test
    void testFactorySentinelOffsets() {
        // The factory must yield exactly the shared sentinels (compareTo equality, not a copy).
        Offset initial = factory.createInitialOffset();
        Offset noStopping = factory.createNoStoppingOffset();
        assertEquals(KafkaJsonOffset.INITIAL_OFFSET, initial);
        assertEquals(KafkaJsonOffset.NO_STOPPING_OFFSET, noStopping);
        assertTrue(initial.isBefore(noStopping));
        assertTrue(noStopping.isAfter(factory.createTimestampOffset(Long.MAX_VALUE - 1)));
    }

    @Test
    void testFactoryCreateTimestampOffset() {
        KafkaJsonOffset offset = (KafkaJsonOffset) factory.createTimestampOffset(987654321L);
        assertEquals(987654321L, offset.getEventTime());
        assertEquals(-1, offset.getPartition());
        assertEquals(-1L, offset.getOffsetValue());
    }

    @Test
    void testFactoryRejectsBinlogStyleOffsets() {
        // This connector tracks Kafka positions, not MySQL binlog positions: the canal-style
        // (filename, position) and (position) factory entry points must not be silently usable.
        assertThrows(FlinkRuntimeException.class, () -> factory.newOffset("binlog.000001", 123L));
        assertThrows(FlinkRuntimeException.class, () -> factory.newOffset(123L));
    }
}
