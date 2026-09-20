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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittable;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittableSerializer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommitter;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterStateSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;

/**
 * The {@code sink.writer=stateful-2pc} Doris sink: rows become visible when the checkpoint they
 * belong to completes, rather than when they are flushed.
 *
 * <p>One class implements both {@link StatefulSink} and {@link TwoPhaseCommittingSink}, which is
 * how this had to be written for Flink 1.18.1: the combined {@code TwoPhaseCommittingStatefulSink}
 * interface only exists from 1.19, so before that a sink with state <em>and</em> committables is
 * spelt as the two interfaces. {@code createWriter} then has to return a writer that satisfies both
 * contracts, which is why {@link TwoPhaseDorisSinkWriter} is one class implementing {@code
 * StatefulSinkWriter} and {@code PrecommittingSinkWriter}.
 *
 * <p>Nothing here creates the committer operator: the sink only supplies the committer, and {@code
 * KafkaJsonDataSinkBuilder} attaches the operator that drives it for checkpoints to reach it.
 */
public class TwoPhaseDorisSink
        implements StatefulSink<Event, DorisWriterState>,
                TwoPhaseCommittingSink<Event, DorisCommittable> {

    private final DorisDataSinkOptions options;
    private final ZoneId pipelineZoneId;

    public TwoPhaseDorisSink(DorisDataSinkOptions options, ZoneId pipelineZoneId) {
        this.options = options;
        this.pipelineZoneId = pipelineZoneId;
    }

    @Override
    public TwoPhaseDorisSinkWriter createWriter(Sink.InitContext context) throws IOException {
        return new TwoPhaseDorisSinkWriter(options, pipelineZoneId, context);
    }

    @Override
    public TwoPhaseDorisSinkWriter restoreWriter(
            Sink.InitContext context, Collection<DorisWriterState> states) throws IOException {
        TwoPhaseDorisSinkWriter writer =
                new TwoPhaseDorisSinkWriter(options, pipelineZoneId, context);
        writer.restoreState(states);
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return new DorisWriterStateSerializer();
    }

    @Override
    public Committer<DorisCommittable> createCommitter() throws IOException {
        return new DorisCommitter(options);
    }

    @Override
    public SimpleVersionedSerializer<DorisCommittable> getCommittableSerializer() {
        return new DorisCommittableSerializer();
    }
}
