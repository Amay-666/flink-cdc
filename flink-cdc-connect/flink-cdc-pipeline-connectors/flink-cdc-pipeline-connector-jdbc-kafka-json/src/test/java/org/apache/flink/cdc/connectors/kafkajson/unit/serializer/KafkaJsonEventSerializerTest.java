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
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the connector serialization stack: {@link KafkaJsonEventTypeInfo} produces {@link
 * KafkaJsonEventSerializer}, which must round-trip the connector's custom events alongside the
 * released event types.
 *
 * <p>Two properties of the string fields are pinned here because the events are serialized on the
 * wire — {@code PrePartitionOperator} broadcasts them to every sink subtask through a Shuffle — and
 * the DDL statement is {@code null} whenever the source message did not carry one (see {@code
 * KafkaJsonEventDeserializer}, which reads it with a lookup that yields {@code null} for an absent
 * field):
 *
 * <ul>
 *   <li>a {@code null} DDL statement must survive, and stay distinct from an empty one;
 *   <li>a DDL statement longer than the 64 KiB that {@code DataOutputView#writeUTF} can encode must
 *       survive — a DDL statement is one string and its length is not bounded by this connector.
 * </ul>
 *
 * <p>Both hold because the string fields go through {@link
 * org.apache.flink.api.common.typeutils.base.StringSerializer}, which delegates to Flink's {@code
 * StringValue#writeString}/{@code readString}: the length is a varint offset by one, so zero means
 * {@code null} and long strings are not capped. Swapping in {@code writeUTF} — the obvious-looking
 * simplification — would break both, which is what these tests are here to catch.
 */
public class KafkaJsonEventSerializerTest {

    /** Longer than the 64 KiB an {@code writeUTF} encoding could carry. */
    private static final int LONG_DDL_LENGTH = 200_000;

    @Test
    public void testProducedTypeIsKafkaJsonEventTypeInfo() {
        assertThat(new KafkaJsonEventTypeInfo().toString()).isEqualTo("KafkaJsonEvent");
    }

    @Test
    public void testRenameTableEventRoundTrip() throws Exception {
        RenameTableEvent original =
                new RenameTableEvent(
                        TableId.tableId("test", "users"),
                        TableId.tableId("test", "vip_users"),
                        schema(),
                        "RENAME TABLE `test`.`users` TO `test`.`vip_users`");

        Event restored = roundTrip(original);

        assertThat(restored).isInstanceOf(RenameTableEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testTruncateTableEventRoundTrip() throws Exception {
        TruncateTableEvent original =
                new TruncateTableEvent(
                        TableId.tableId("test", "users"),
                        schema(),
                        "TRUNCATE TABLE `test`.`users`");

        Event restored = roundTrip(original);

        assertThat(restored).isInstanceOf(TruncateTableEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testAlterColumnCommentEventRoundTrip() throws Exception {
        AlterColumnCommentEvent original =
                new AlterColumnCommentEvent(
                        TableId.tableId("test", "users"),
                        Collections.singletonMap("name", "nickname"));

        Event restored = roundTrip(original);

        assertThat(restored).isInstanceOf(AlterColumnCommentEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testDropTableEventRoundTrip() throws Exception {
        DropTableEvent original =
                new DropTableEvent(
                        TableId.tableId("test", "users"), schema(), "DROP TABLE `test`.`users`");

        Event restored = roundTrip(original);

        assertThat(restored).isInstanceOf(DropTableEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testAlterTableCommentEventRoundTrip() throws Exception {
        AlterTableCommentEvent original =
                new AlterTableCommentEvent(
                        TableId.tableId("test", "users"),
                        schema(),
                        "ALTER TABLE `test`.`users` COMMENT = 'vip users'",
                        "vip users");

        Event restored = roundTrip(original);

        assertThat(restored).isInstanceOf(AlterTableCommentEvent.class);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testNullDdlStatementSurvivesRoundTrip() throws Exception {
        // A source message that carries no DDL statement leaves the field null, and the event is
        // still broadcast and serialized like any other.
        RenameTableEvent rename =
                new RenameTableEvent(
                        TableId.tableId("test", "users"),
                        TableId.tableId("test", "vip_users"),
                        schema());
        TruncateTableEvent truncate =
                new TruncateTableEvent(TableId.tableId("test", "users"), schema());
        DropTableEvent drop = new DropTableEvent(TableId.tableId("test", "users"), schema(), null);
        AlterTableCommentEvent comment =
                new AlterTableCommentEvent(
                        TableId.tableId("test", "users"), schema(), null, "vip users");

        assertThat(((RenameTableEvent) roundTrip(rename)).getSql()).isNull();
        assertThat(roundTrip(rename)).isEqualTo(rename);

        assertThat(((TruncateTableEvent) roundTrip(truncate)).getSql()).isNull();
        assertThat(roundTrip(truncate)).isEqualTo(truncate);

        assertThat(((DropTableEvent) roundTrip(drop)).getSql()).isNull();
        assertThat(roundTrip(drop)).isEqualTo(drop);

        assertThat(((AlterTableCommentEvent) roundTrip(comment)).getSql()).isNull();
        assertThat(roundTrip(comment)).isEqualTo(comment);
    }

    @Test
    public void testNullDdlStatementStaysDistinctFromEmptyOne() throws Exception {
        // The encoding writes null as a zero length, so an empty statement must not decode as null:
        // the coordinator compares replayed events with equals, and conflating the two would make a
        // replayed event look different from the one already applied.
        TruncateTableEvent withNull =
                new TruncateTableEvent(TableId.tableId("test", "users"), schema());
        TruncateTableEvent withEmpty =
                new TruncateTableEvent(TableId.tableId("test", "users"), schema(), "");

        assertThat(((TruncateTableEvent) roundTrip(withNull)).getSql()).isNull();
        assertThat(((TruncateTableEvent) roundTrip(withEmpty)).getSql()).isEmpty();
        assertThat(roundTrip(withEmpty)).isNotEqualTo(roundTrip(withNull));
    }

    @Test
    public void testLongDdlStatementSurvivesRoundTrip() throws Exception {
        String longSql = repeat('d', LONG_DDL_LENGTH);
        TruncateTableEvent original =
                new TruncateTableEvent(TableId.tableId("test", "users"), schema(), longSql);

        byte[] bytes = serialize(original);
        assertThat(bytes.length).isGreaterThan(0xFFFF);

        TruncateTableEvent restored = (TruncateTableEvent) roundTrip(original);

        assertThat(restored.getSql()).hasSize(LONG_DDL_LENGTH);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void testLongCommentSurvivesRoundTrip() throws Exception {
        // The comment fields are written with the same string encoding, so they carry the same
        // length bound; a column comment is the larger of the two in practice.
        String longComment = repeat('c', LONG_DDL_LENGTH);
        AlterTableCommentEvent tableComment =
                new AlterTableCommentEvent(
                        TableId.tableId("test", "users"), schema(), null, longComment);
        AlterColumnCommentEvent columnComment =
                new AlterColumnCommentEvent(
                        TableId.tableId("test", "users"),
                        Collections.singletonMap("name", longComment));

        assertThat(((AlterTableCommentEvent) roundTrip(tableComment)).getComment())
                .hasSize(LONG_DDL_LENGTH);
        assertThat(roundTrip(tableComment)).isEqualTo(tableComment);

        assertThat(
                        ((AlterColumnCommentEvent) roundTrip(columnComment))
                                .getCommentMapping()
                                .get("name"))
                .hasSize(LONG_DDL_LENGTH);
        assertThat(roundTrip(columnComment)).isEqualTo(columnComment);
    }

    private static Event roundTrip(Event original) throws Exception {
        TypeSerializer<Event> serializer =
                new KafkaJsonEventTypeInfo().createSerializer(new ExecutionConfig());
        Event restored =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(serialize(original))));
        // A format that silently dropped or truncated a field would still decode, so check the
        // encoding is stable too: re-encoding the restored event must reproduce the same bytes.
        assertThat(serialize(restored)).isEqualTo(serialize(original));
        return restored;
    }

    private static byte[] serialize(Event event) throws Exception {
        TypeSerializer<Event> serializer =
                new KafkaJsonEventTypeInfo().createSerializer(new ExecutionConfig());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(event, new DataOutputViewStreamWrapper(baos));
        return baos.toByteArray();
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .setColumns(
                        Collections.singletonList(
                                Column.physicalColumn("id", DataTypes.BIGINT(), null)))
                .primaryKey(Collections.singletonList("id"))
                .build();
    }

    private static String repeat(char c, int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
