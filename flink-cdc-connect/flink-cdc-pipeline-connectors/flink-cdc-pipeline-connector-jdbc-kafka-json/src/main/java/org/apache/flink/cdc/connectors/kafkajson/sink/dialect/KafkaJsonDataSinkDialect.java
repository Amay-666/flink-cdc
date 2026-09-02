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

package org.apache.flink.cdc.connectors.kafkajson.sink.dialect;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DefaultDataChangeEventHashFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.converter.KafkaJsonRowConverter;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * Abstraction of a sink dialect for the kafka-json connector.
 *
 * <p>The connector deliberately does <em>not</em> register a released {@code DataSinkFactory}: the
 * user-side job assembles the DataStream itself via {@code KafkaJsonDataSinkBuilder}, so this
 * dialect is the seam between the shared plumbing (the released schema-evolution / writer operators
 * and the coordination protocol) and the concrete target system. A dialect supplies the four pieces
 * that are target-specific:
 *
 * <ul>
 *   <li>the {@link Sink} driven by the released {@code DataSinkWriterOperator};
 *   <li>the {@link MetadataApplier} that executes DDL during schema evolution;
 *   <li>the {@link KafkaJsonRowConverter} that renders {@code RecordData} rows for the target;
 *   <li>the {@link HashFunctionProvider} that keys data rows for partitioning.
 * </ul>
 */
public abstract class KafkaJsonDataSinkDialect implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final KafkaJsonDataSinkOptions options;

    protected KafkaJsonDataSinkDialect(KafkaJsonDataSinkOptions options) {
        this.options = options;
    }

    public KafkaJsonDataSinkOptions getOptions() {
        return options;
    }

    /** Creates the event sink that the released {@code DataSinkWriterOperator} drives. */
    public abstract Sink<Event> createSink();

    /** Creates the metadata applier that applies schema changes to the target system. */
    public abstract MetadataApplier createMetadataApplier();

    /** Creates a row converter for the given table schema. */
    public abstract KafkaJsonRowConverter createRowConverter(Schema schema, ZoneId pipelineZoneId);

    /**
     * Creates the hash function provider used to partition data change events. The default keeps
     * rows of the same (table original name, primary key) on the same subtask — the {@code key by
     * 表原始名称 + 主键 id} requirement — so a large table is spread by primary key instead of
     * monopolizing a single subtask. Dialects may override to change the keying.
     */
    public HashFunctionProvider<DataChangeEvent> createHashFunctionProvider() {
        return new DefaultDataChangeEventHashFunctionProvider();
    }
}
