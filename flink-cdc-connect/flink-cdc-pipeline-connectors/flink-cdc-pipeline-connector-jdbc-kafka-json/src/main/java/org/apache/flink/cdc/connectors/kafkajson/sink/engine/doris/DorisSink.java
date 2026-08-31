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
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.Event;

import java.time.ZoneId;

/**
 * A non-2PC {@link Sink} that writes CDC {@link Event}s to Doris via StreamLoad.
 *
 * <p>Because the sink is not a {@code TwoPhaseCommittingSink}, there is no precommit/commit phase:
 * a StreamLoad PUT is the commit. The released {@code DataSinkWriterOperator} drives this sink's
 * {@link DorisSinkWriter} and feeds it {@code FlushEvent}s (mapped to a forced flush) and
 * schema-change events, so the writer is the per-subtask data landing point of the DDL-blocking
 * protocol.
 */
public class DorisSink implements Sink<Event> {

    private static final long serialVersionUID = 1L;

    private final DorisDataSinkOptions options;
    private final ZoneId pipelineZoneId;

    public DorisSink(DorisDataSinkOptions options, ZoneId pipelineZoneId) {
        this.options = options;
        this.pipelineZoneId = pipelineZoneId;
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) {
        return new DorisSinkWriter(options, pipelineZoneId, context);
    }
}
