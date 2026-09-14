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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.schema.coordinator;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaManager;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link KafkaJsonSchemaManager}, focused on the lifecycle of a table id: the schema
 * entry of a dropped or renamed-away table is kept (the released {@code SchemaOperator} refreshes
 * its caches under that id after the event), so the manager has to remember separately that the id
 * is free again. Without that memory the redundancy check discards the {@code CREATE TABLE} — or
 * the rename back — of a table the external system no longer has.
 */
public class KafkaJsonSchemaManagerTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ARCHIVE = TableId.tableId("shop", "orders_archive");

    private static final Schema SCHEMA_V1 =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();
    private static final Schema SCHEMA_V2 =
            Schema.newBuilder().physicalColumn("id", DataTypes.BIGINT()).primaryKey("id").build();

    private final KafkaJsonSchemaManager manager =
            new KafkaJsonSchemaManager(SchemaChangeBehavior.EVOLVE);

    @Test
    public void testDuplicateCreateTableIsRedundant() {
        CreateTableEvent createTable = new CreateTableEvent(ORDERS, SCHEMA_V1);
        apply(createTable);

        assertThat(manager.isOriginalSchemaChangeEventRedundant(createTable)).isTrue();
    }

    @Test
    public void testCreateTableAfterDropTableIsAppliedAndStartsAFreshHistory() {
        CreateTableEvent createTable = new CreateTableEvent(ORDERS, SCHEMA_V1);
        apply(createTable);
        apply(new DropTableEvent(ORDERS, SCHEMA_V1, null));

        // The entry of the dropped table is still there, but the table is gone: the CREATE TABLE
        // of the same name is a new table and must not be discarded as a duplicate.
        CreateTableEvent recreated = new CreateTableEvent(ORDERS, SCHEMA_V2);
        assertThat(manager.isOriginalSchemaChangeEventRedundant(recreated)).isFalse();

        apply(recreated);

        assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA_V2);
        assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA_V2);
        // The history of the dropped table is gone: the re-created table owns version 0 and has no
        // version 1 yet. Keeping SCHEMA_V1 around would let a sink resolve a schema version the
        // re-created table never had.
        assertThat(manager.getOriginalSchema(ORDERS, 0)).isEqualTo(SCHEMA_V2);
        assertThat(manager.getEvolvedSchema(ORDERS, 0)).isEqualTo(SCHEMA_V2);
        assertThatThrownBy(() -> manager.getOriginalSchema(ORDERS, 1))
                .isInstanceOf(IllegalArgumentException.class);
        // Duplicates of the re-creation are deduplicated again.
        assertThat(manager.isOriginalSchemaChangeEventRedundant(recreated)).isTrue();
    }

    @Test
    public void testRenameBackToTheOldTableIdIsApplied() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        RenameTableEvent renameAway = new RenameTableEvent(ORDERS, ARCHIVE, SCHEMA_V1);
        apply(renameAway);
        assertThat(manager.isOriginalSchemaChangeEventRedundant(renameAway)).isTrue();

        // No table is called `orders` any more, so renaming the archive back has to be applied even
        // though the `orders` entry — kept for the SchemaOperator's cache refresh — still exists.
        RenameTableEvent renameBack = new RenameTableEvent(ARCHIVE, ORDERS, SCHEMA_V2);
        assertThat(manager.isOriginalSchemaChangeEventRedundant(renameBack)).isFalse();

        apply(renameBack);

        assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA_V2);
        assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA_V2);
        assertThat(manager.getEvolvedSchema(ORDERS, 0)).isEqualTo(SCHEMA_V2);
    }

    @Test
    public void testRenameOntoADroppedTableIdIsApplied() {
        apply(new CreateTableEvent(ARCHIVE, SCHEMA_V1));
        apply(new DropTableEvent(ARCHIVE, SCHEMA_V1, null));

        RenameTableEvent rename = new RenameTableEvent(ORDERS, ARCHIVE, SCHEMA_V2);
        assertThat(manager.isOriginalSchemaChangeEventRedundant(rename)).isFalse();

        apply(rename);

        assertThat(manager.getLatestEvolvedSchema(ARCHIVE)).contains(SCHEMA_V2);
        // The dropped table's single version is replaced, not appended to.
        assertThat(manager.getEvolvedSchema(ARCHIVE, 0)).isEqualTo(SCHEMA_V2);
    }

    @Test
    public void testDropAndTruncateAreNeverRedundant() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        DropTableEvent dropTable = new DropTableEvent(ORDERS, SCHEMA_V1, null);
        TruncateTableEvent truncateTable = new TruncateTableEvent(ORDERS, SCHEMA_V1);

        // Both DDLs are idempotent, so a replay is harmless and never filtered out.
        assertThat(manager.isOriginalSchemaChangeEventRedundant(dropTable)).isFalse();
        assertThat(manager.isOriginalSchemaChangeEventRedundant(truncateTable)).isFalse();
    }

    @Test
    public void testTruncateKeepsTheSchema() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        apply(new TruncateTableEvent(ORDERS, SCHEMA_V1));

        assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA_V1);
        assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA_V1);
        // The table still exists, so a later CREATE TABLE of the same name stays a duplicate.
        assertThat(
                        manager.isOriginalSchemaChangeEventRedundant(
                                new CreateTableEvent(ORDERS, SCHEMA_V1)))
                .isTrue();
    }

    @Test
    public void testRemovedTableIdsSurviveCheckpointRoundTrip() throws Exception {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        apply(new DropTableEvent(ORDERS, SCHEMA_V1, null));

        KafkaJsonSchemaManager restored = roundTrip(manager);

        // Equality covers the removed-table set, so this also proves it made it into the bytes.
        assertThat(restored).isEqualTo(manager);
        // Without the restored set, a failover would swallow the re-creation again.
        assertThat(
                        restored.isOriginalSchemaChangeEventRedundant(
                                new CreateTableEvent(ORDERS, SCHEMA_V2)))
                .isFalse();
    }

    @Test
    public void testDroppedTableMakesManagersDiffer() throws Exception {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        KafkaJsonSchemaManager beforeDrop = roundTrip(manager);
        apply(new DropTableEvent(ORDERS, SCHEMA_V1, null));

        assertThat(roundTrip(manager)).isNotEqualTo(beforeDrop);
    }

    @Test
    public void testColumnCommentOnAnUnknownColumnIsIgnoredWithoutAVersionBump() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));

        apply(new AlterColumnCommentEvent(ORDERS, Collections.singletonMap("missing", "c")));

        // The event names a column the table does not have, so it cannot change the schema. The
        // manager must not pretend it did: the entry keeps its single version 0.
        assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA_V1);
        assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA_V1);
        assertThat(manager.getOriginalSchema(ORDERS, 0)).isEqualTo(SCHEMA_V1);
        assertThatThrownBy(() -> manager.getOriginalSchema(ORDERS, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testReplayedUnknownColumnCommentDoesNotEvictARetainedVersion() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        AlterColumnCommentEvent bogus =
                new AlterColumnCommentEvent(ORDERS, Collections.singletonMap("missing", "c"));

        // The column never appears, so the event can never be recognised as already applied and is
        // replayed on every restart. Registering the unchanged schema each time would pile up one
        // version per replay and push version 0 — the only one a sink can resolve for the
        // pre-comment schema — out of the retained window.
        apply(bogus);
        apply(bogus);
        apply(bogus);

        assertThat(manager.getOriginalSchema(ORDERS, 0)).isEqualTo(SCHEMA_V1);
        assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA_V1);
    }

    @Test
    public void testChangedTableCommentStillRegistersANewVersion() {
        apply(new CreateTableEvent(ORDERS, SCHEMA_V1));
        apply(new AlterTableCommentEvent(ORDERS, SCHEMA_V1, null, "first"));

        // A real change still moves on to the next version — the guard only suppresses no-ops.
        assertThat(manager.getOriginalSchema(ORDERS, 1).comment()).isEqualTo("first");
        assertThat(manager.getLatestOriginalSchema(ORDERS))
                .hasValueSatisfying(schema -> assertThat(schema.comment()).isEqualTo("first"));
    }

    /** Applies an event the way the request handler does: original schema first, evolved last. */
    private void apply(SchemaChangeEvent event) {
        manager.applyOriginalSchemaChange(event);
        manager.applyEvolvedSchemaChange(event);
    }

    private static KafkaJsonSchemaManager roundTrip(KafkaJsonSchemaManager schemaManager)
            throws IOException {
        byte[] serialized = KafkaJsonSchemaManager.SERIALIZER.serialize(schemaManager);
        return KafkaJsonSchemaManager.SERIALIZER.deserialize(
                KafkaJsonSchemaManager.SERIALIZER.getVersion(), serialized);
    }
}
