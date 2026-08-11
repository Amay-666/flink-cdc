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

package org.apache.flink.cdc.connectors.kafkajson.serializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;

/**
 * A {@link EventTypeInfo} that creates {@link KafkaJsonEventSerializer} instead of the released {@code
 * EventSerializer}, so that a {@link org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent}
 * can be (de)serialized.
 *
 * <p>The canal source produces this type via {@code KafkaJsonEventDeserializer.getProducedType()}; in a
 * job that uses the connector's own serialization stack end to end, the whole stream carries this
 * type information and no released serializer is ever asked to see the new event.
 *
 * <p>{@code equals}/{@code hashCode} are overridden to keep them mutually consistent (the released
 * {@code EventTypeInfo} treats every subclass as equal but hashes by concrete class, which would
 * violate the equals/hashCode contract for a subclass).
 */
public class KafkaJsonEventTypeInfo extends EventTypeInfo {

    private static final long serialVersionUID = 1L;

    @Override
    public TypeSerializer<Event> createSerializer(ExecutionConfig config) {
        return KafkaJsonEventSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "KafkaJsonEvent";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof KafkaJsonEventTypeInfo;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof KafkaJsonEventTypeInfo;
    }
}
