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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterStateSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;

/**
 * The {@code sink.writer=stateful} Doris sink: single-phase Stream Load with a checkpointed
 * sequence counter, i.e. {@link StatefulDorisSinkWriter} plus the state plumbing Flink needs around
 * it.
 *
 * <p>Restoring is done here rather than by the writer, because that is where Flink 1.18.1 asks for
 * it: {@code StatefulSink} owns {@code restoreWriter} and hands the redistributed states to the
 * sink, which creates the writer and replays them into it.
 *
 * <p>This sink deliberately does <em>not</em> implement {@code TwoPhaseCommittingSink}. The
 * operator decides whether to emit committables by checking for that interface, so a sink that
 * implements it has to produce them; a single-phase writer has none, and the committer stream would
 * be built to receive nothing. {@link TwoPhaseDorisSink} is the sink that does implement it.
 */
public class StatefulDorisSink implements StatefulSink<Event, DorisWriterState> {

    private final DorisDataSinkOptions options;
    private final ZoneId pipelineZoneId;

    public StatefulDorisSink(DorisDataSinkOptions options, ZoneId pipelineZoneId) {
        this.options = options;
        this.pipelineZoneId = pipelineZoneId;
    }

    @Override
    public StatefulDorisSinkWriter createWriter(Sink.InitContext context) throws IOException {
        return new StatefulDorisSinkWriter(options, pipelineZoneId, context);
    }

    @Override
    public StatefulDorisSinkWriter restoreWriter(
            Sink.InitContext context, Collection<DorisWriterState> states) throws IOException {
        StatefulDorisSinkWriter writer =
                new StatefulDorisSinkWriter(options, pipelineZoneId, context);
        writer.restoreState(states);
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return new DorisWriterStateSerializer();
    }
}
