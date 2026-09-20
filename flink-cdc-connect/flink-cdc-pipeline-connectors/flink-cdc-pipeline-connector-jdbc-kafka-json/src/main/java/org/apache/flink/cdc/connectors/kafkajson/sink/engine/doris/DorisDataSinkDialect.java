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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.sink.converter.KafkaJsonRowConverter;
import org.apache.flink.cdc.connectors.kafkajson.sink.dialect.KafkaJsonDataSinkDialect;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisMetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.DorisSink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.StatefulDorisSink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.TwoPhaseDorisSink;

import java.time.ZoneId;

/**
 * The Doris dialect of the kafka-json sink.
 *
 * <p>Wires the connector's shared plumbing to the HTTP-only Doris stack: {@link DorisSink} as the
 * event sink, {@link DorisMetadataApplier} for DDL execution and {@link DorisRowConverter} for row
 * rendering. The pipeline zone id is used both for the sink (time-zone aware timestamp rendering)
 * and, when building the row converter, should be passed as the {@code pipelineZoneId} argument.
 */
public class DorisDataSinkDialect extends KafkaJsonDataSinkDialect {

    private static final long serialVersionUID = 1L;

    private final ZoneId pipelineZoneId;

    public DorisDataSinkDialect(DorisDataSinkOptions options, ZoneId pipelineZoneId) {
        super(options);
        this.pipelineZoneId = pipelineZoneId;
    }

    public DorisDataSinkDialect(DorisDataSinkOptions options) {
        this(options, ZoneId.systemDefault());
    }

    @Override
    public DorisDataSinkOptions getOptions() {
        return (DorisDataSinkOptions) super.getOptions();
    }

    /**
     * Builds the sink the configured {@code sink.writer} mode asks for.
     *
     * <p>The two-phase sink is returned as a plain {@link Sink} like the others; that it also
     * implements {@code TwoPhaseCommittingSink} is discovered downstream, by the sink builder,
     * which is where the committer operator has to be attached to a stream to exist at all.
     */
    @Override
    public Sink<Event> createSink() {
        switch (getOptions().getWriterMode()) {
            case STATEFUL:
                return new StatefulDorisSink(getOptions(), pipelineZoneId);
            case STATEFUL_2PC:
                return new TwoPhaseDorisSink(getOptions(), pipelineZoneId);
            case LEGACY:
            default:
                return new DorisSink(getOptions(), pipelineZoneId);
        }
    }

    @Override
    public MetadataApplier createMetadataApplier() {
        return new DorisMetadataApplier(getOptions());
    }

    @Override
    public KafkaJsonRowConverter createRowConverter(Schema schema, ZoneId pipelineZoneId) {
        return new DorisRowConverter(schema, pipelineZoneId);
    }
}
