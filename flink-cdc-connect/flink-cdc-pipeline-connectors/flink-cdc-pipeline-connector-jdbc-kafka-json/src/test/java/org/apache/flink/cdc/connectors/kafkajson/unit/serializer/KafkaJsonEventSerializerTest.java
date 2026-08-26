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

package org.apache.flink.cdc.connectors.kafkajson.unit.serializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the canal serialization stack: {@link KafkaJsonEventTypeInfo} produces {@link
 * KafkaJsonEventSerializer}, which must round-trip a {@link RenameTableEvent} alongside the
 * released event types.
 */
public class KafkaJsonEventSerializerTest {

    @Test
    public void testProducedTypeIsKafkaJsonEventTypeInfo() {
        assertThat(new KafkaJsonEventTypeInfo().toString()).isEqualTo("KafkaJsonEvent");
    }

    @Test
    public void testRenameTableEventRoundTrip() throws Exception {
        TypeSerializer<Event> serializer =
                new KafkaJsonEventTypeInfo().createSerializer(new ExecutionConfig());

        RenameTableEvent original =
                new RenameTableEvent(
                        TableId.tableId("test", "users"),
                        TableId.tableId("test", "vip_users"),
                        Schema.newBuilder()
                                .setColumns(
                                        Collections.singletonList(
                                                Column.physicalColumn(
                                                        "id", DataTypes.BIGINT(), null)))
                                .primaryKey(Collections.singletonList("id"))
                                .build(),
                        "RENAME TABLE `test`.`users` TO `test`.`vip_users`");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(original, new DataOutputViewStreamWrapper(baos));
        Event restored =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(baos.toByteArray())));

        assertThat(restored).isInstanceOf(RenameTableEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testTruncateTableEventRoundTrip() throws Exception {
        TypeSerializer<Event> serializer =
                new KafkaJsonEventTypeInfo().createSerializer(new ExecutionConfig());

        TruncateTableEvent original =
                new TruncateTableEvent(
                        TableId.tableId("test", "users"),
                        Schema.newBuilder()
                                .setColumns(
                                        Collections.singletonList(
                                                Column.physicalColumn(
                                                        "id", DataTypes.BIGINT(), null)))
                                .primaryKey(Collections.singletonList("id"))
                                .build(),
                        "TRUNCATE TABLE `test`.`users`");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(original, new DataOutputViewStreamWrapper(baos));
        Event restored =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(baos.toByteArray())));

        assertThat(restored).isInstanceOf(TruncateTableEvent.class);
        assertThat(restored).isEqualTo(original);
    }
}
