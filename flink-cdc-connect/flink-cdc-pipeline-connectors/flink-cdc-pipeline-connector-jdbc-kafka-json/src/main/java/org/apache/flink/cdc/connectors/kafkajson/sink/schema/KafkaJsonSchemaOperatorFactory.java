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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaRegistryProvider;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;

import java.time.Duration;
import java.util.Collections;

/**
 * Factory to create the schema operator for the kafka-json connector.
 *
 * <p>Reuses the released {@link SchemaOperator} (its {@code getType()} call is only reachable in
 * EXCEPTION mode, so with {@link SchemaChangeBehavior#EVOLVE} the connector's custom schema-change
 * events are handled safely) and wires the coordinator through the connector's {@link
 * KafkaJsonSchemaRegistryProvider}, which understands all ten events.
 */
public class KafkaJsonSchemaOperatorFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {

    private static final long serialVersionUID = 1L;

    private final MetadataApplier metadataApplier;
    private final SchemaChangeBehavior schemaChangeBehavior;

    public KafkaJsonSchemaOperatorFactory(
            MetadataApplier metadataApplier,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        super(
                new SchemaOperator(
                        Collections.emptyList(), rpcTimeOut, schemaChangeBehavior, timezone));
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new KafkaJsonSchemaRegistryProvider(
                operatorID, operatorName, metadataApplier, schemaChangeBehavior);
    }
}
