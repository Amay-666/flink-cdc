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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonOffsetFactory}. */
class KafkaJsonOffsetFactoryTest {

    private final KafkaJsonOffsetFactory factory = new KafkaJsonOffsetFactory();

    @Test
    void testNewOffsetFromMap() {
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
    void testCreateInitialOffset() {
        Offset offset = factory.createInitialOffset();
        assertEquals(KafkaJsonOffset.INITIAL_OFFSET, offset);
        assertTrue(offset.isBefore(factory.createNoStoppingOffset()));
    }

    @Test
    void testCreateNoStoppingOffset() {
        Offset offset = factory.createNoStoppingOffset();
        assertEquals(KafkaJsonOffset.NO_STOPPING_OFFSET, offset);
        assertTrue(offset.isAfter(factory.createTimestampOffset(Long.MAX_VALUE - 1)));
    }

    @Test
    void testCreateTimestampOffset() {
        KafkaJsonOffset offset = (KafkaJsonOffset) factory.createTimestampOffset(987654321L);
        assertEquals(987654321L, offset.getEventTime());
        assertEquals(-1, offset.getPartition());
        assertEquals(-1L, offset.getOffsetValue());
    }

    @Test
    void testUnsupportedNewOffsetWithFilename() {
        assertThrows(FlinkRuntimeException.class, () -> factory.newOffset("binlog.000001", 123L));
    }

    @Test
    void testUnsupportedNewOffsetWithPosition() {
        assertThrows(FlinkRuntimeException.class, () -> factory.newOffset(123L));
    }
}
