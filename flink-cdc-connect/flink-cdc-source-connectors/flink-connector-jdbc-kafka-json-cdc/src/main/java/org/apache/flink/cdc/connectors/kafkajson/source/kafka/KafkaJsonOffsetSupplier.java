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

package org.apache.flink.cdc.connectors.kafkajson.source.kafka;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supplies the current position of the change-log stream, the {@link KafkaJsonOffset} that the snapshot
 * phase uses as the low/high watermark.
 *
 * <p>The position is the maximum canal event time ({@code es}/{@code ts}) of the newest message in
 * each Kafka partition, stamped onto the sentinel partition/offset (see {@link
 * KafkaJsonKafkaOffsetUtils}): any change whose event time is at or before that position was
 * committed while the snapshot was running and is replayed exactly once by the snapshot split's
 * bounded backfill, while the stream phase reads only event times strictly after it. A change
 * committed during a snapshot split's JDBC read therefore never falls through to the stream side,
 * where the snapshot rows would already contain its effect and it would be emitted twice.
 *
 * <p>A single {@link KafkaConsumer} is opened lazily and reused across {@link #current()} calls for
 * the whole reader lifetime; call {@link #close()} when the reader shuts down.
 */
public class KafkaJsonOffsetSupplier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonOffsetSupplier.class);

    private final KafkaJsonSourceConfig sourceConfig;
    private final KafkaConsumer<String, String> consumer;

    public KafkaJsonOffsetSupplier(KafkaJsonSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.consumer = new KafkaConsumer<>(KafkaJsonKafkaOffsetUtils.buildConsumerProps(sourceConfig));
    }

    /** Constructor with an externally provided consumer (used in unit tests). */
    @VisibleForTesting
    KafkaJsonOffsetSupplier(KafkaJsonSourceConfig sourceConfig, KafkaConsumer<String, String> consumer) {
        this.sourceConfig = sourceConfig;
        this.consumer = consumer;
    }

    /**
     * Returns the current position of the change-log stream; {@link KafkaJsonOffset#INITIAL_OFFSET} if
     * no message is available yet.
     */
    public KafkaJsonOffset current() {
        return KafkaJsonKafkaOffsetUtils.queryCurrentOffset(
                consumer, sourceConfig.getKafkaTopics(), sourceConfig.getEventTime());
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.warn("Failed to close the KafkaJsonOffsetSupplier consumer", e);
        }
    }
}
