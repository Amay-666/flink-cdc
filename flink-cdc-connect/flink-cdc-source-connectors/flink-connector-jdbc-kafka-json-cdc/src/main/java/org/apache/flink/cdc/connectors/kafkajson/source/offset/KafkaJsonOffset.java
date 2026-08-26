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

import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * The offset for the Canal source.
 *
 * <p>Kafka does not have a globally monotonic position, so we use the <b>message event time</b> as
 * the primary ordering key, with {@code (partition, offset)} as the tie-breaker so that messages in
 * the same partition stay strictly ordered. The event time is either the binlog execution time
 * ({@code es}) or the canal send time ({@code ts}) of the canal flatMessage, controlled by {@code
 * scan.message.event-time}.
 *
 * <p>The comparison order is: {@code eventTime -> partition -> offset}.
 */
public class KafkaJsonOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String EVENT_TIME_KEY = "eventTime";
    public static final String PARTITION_KEY = "partition";
    public static final String OFFSET_KEY = "offset";

    /** The offset that is smaller than any real offset. */
    public static final KafkaJsonOffset INITIAL_OFFSET = new KafkaJsonOffset(-1L, -1, -1L);

    /** The offset that is greater than any real offset. */
    public static final KafkaJsonOffset NO_STOPPING_OFFSET =
            new KafkaJsonOffset(Long.MAX_VALUE, -1, -1L);

    // used by KafkaJsonOffsetFactory
    KafkaJsonOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public KafkaJsonOffset(long eventTime, int partition, long offset) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(EVENT_TIME_KEY, String.valueOf(eventTime));
        offsetMap.put(PARTITION_KEY, String.valueOf(partition));
        offsetMap.put(OFFSET_KEY, String.valueOf(offset));
        this.offset = offsetMap;
    }

    public static KafkaJsonOffset of(SourceRecord dataRecord) {
        return of(dataRecord.sourceOffset());
    }

    public static KafkaJsonOffset of(Map<String, ?> offsetMap) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new KafkaJsonOffset(offsetStrMap);
    }

    /** Returns the message event time (millis) of this offset. */
    public long getEventTime() {
        return longOffsetValue(offset, EVENT_TIME_KEY);
    }

    /** Returns the Kafka partition of this offset. */
    public int getPartition() {
        return (int) longOffsetValue(offset, PARTITION_KEY);
    }

    /** Returns the Kafka partition-local offset of this offset. */
    public long getOffsetValue() {
        return longOffsetValue(offset, OFFSET_KEY);
    }

    @Override
    public int compareTo(Offset o) {
        KafkaJsonOffset rhs = (KafkaJsonOffset) o;
        int cmp = Long.compare(getEventTime(), rhs.getEventTime());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(getPartition(), rhs.getPartition());
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(getOffsetValue(), rhs.getOffsetValue());
    }

    @Override
    public String toString() {
        return "Offset{eventTime="
                + getEventTime()
                + ", partition="
                + getPartition()
                + ", offset="
                + getOffsetValue()
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaJsonOffset)) {
            return false;
        }
        KafkaJsonOffset that = (KafkaJsonOffset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int hashCode() {
        return offset.hashCode();
    }
}
