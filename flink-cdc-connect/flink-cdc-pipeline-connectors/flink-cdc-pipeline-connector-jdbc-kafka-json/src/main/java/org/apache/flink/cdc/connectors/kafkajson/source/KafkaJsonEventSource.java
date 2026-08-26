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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.reader.KafkaJsonPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * The pipeline entry point of the Canal source. It reuses the {@link KafkaJsonSource} skeleton and
 * only overrides the {@code createRecordEmitter} hook so that the pipeline record emitter (which
 * lazily emits {@link org.apache.flink.cdc.common.event.CreateTableEvent}s and flushes the
 * schema-change cache) is used instead of the plain {@code IncrementalSourceRecordEmitter}.
 */
@Internal
public class KafkaJsonEventSource extends KafkaJsonSource<Event> {

    private static final long serialVersionUID = 1L;

    public KafkaJsonEventSource(
            KafkaJsonSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<Event> deserializationSchema) {
        super(
                configFactory,
                deserializationSchema,
                new KafkaJsonOffsetFactory(),
                new KafkaJsonDialect(configFactory.create(0)));
    }

    @Override
    protected RecordEmitter<SourceRecords, Event, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        return new KafkaJsonPipelineRecordEmitter(
                deserializationSchema, sourceReaderMetrics, (KafkaJsonSourceConfig) sourceConfig);
    }
}
