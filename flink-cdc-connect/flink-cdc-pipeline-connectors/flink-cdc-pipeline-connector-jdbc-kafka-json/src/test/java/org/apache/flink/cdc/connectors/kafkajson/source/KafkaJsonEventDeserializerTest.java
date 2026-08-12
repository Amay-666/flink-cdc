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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonEventDeserializer}: the schema-change records (produced by the canal
 * DDL handler) are diffed against the internal table registry into column-level schema change
 * events, and the data-change records are turned into {@link DataChangeEvent}s.
 */
public class KafkaJsonEventDeserializerTest {

    private static final TableId TABLE_ID = TableId.tableId("test", "users");

    private static final Table BASE_TABLE =
            Table.editor()
                    .tableId(new io.debezium.relational.TableId("test", null, "users"))
                    .addColumn(
                            Column.editor()
                                    .name("id")
                                    .type("BIGINT")
                                    .jdbcType(Types.BIGINT)
                                    .length(20)
                                    .optional(false)
                                    .position(1)
                                    .create())
                    .addColumn(
                            Column.editor()
                                    .name("name")
                                    .type("VARCHAR")
                                    .jdbcType(Types.VARCHAR)
                                    .length(255)
                                    .optional(true)
                                    .position(2)
                                    .create())
                    .setPrimaryKeyNames("id")
                    .create();

    private final KafkaJsonEventDeserializer deserializer =
            new KafkaJsonEventDeserializer(DebeziumChangelogMode.ALL, true);

    @Test
    public void testCreateTableEvent() throws Exception {
        TableChanges changes = new TableChanges();
        changes.create(BASE_TABLE);

        List<? extends Event> events = deserializer.deserialize(schemaChangeRecord(changes));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent create = (CreateTableEvent) events.get(0);
        assertThat(create.tableId()).isEqualTo(TABLE_ID);
        assertThat(create.getSchema().getColumns()).hasSize(2);
        assertThat(create.getSchema().primaryKeys()).containsExactly("id");
    }

    @Test
    public void testAlterAddColumn() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("name", "VARCHAR", Types.VARCHAR, true, 2, 255))
                        .addColumn(column("age", "INT", Types.INTEGER, true, 3))
                        .setPrimaryKeyNames("id")
                        .create();

        List<? extends Event> events = deserializer.deserialize(alterRecord(newTable));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent add = (AddColumnEvent) events.get(0);
        assertThat(add.tableId()).isEqualTo(TABLE_ID);
        assertThat(add.getAddedColumns()).hasSize(1);
        assertThat(add.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("age");
        assertThat(add.getAddedColumns().get(0).getAddColumn().getType().toString())
                .isEqualTo("INT");
    }

    @Test
    public void testAlterColumnType() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("name", "VARCHAR", Types.VARCHAR, true, 2, 300))
                        .setPrimaryKeyNames("id")
                        .create();

        List<? extends Event> events = deserializer.deserialize(alterRecord(newTable));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent alter = (AlterColumnTypeEvent) events.get(0);
        assertThat(alter.tableId()).isEqualTo(TABLE_ID);
        Map<String, DataType> typeMapping = alter.getTypeMapping();
        assertThat(typeMapping).containsOnlyKeys("name");
        assertThat(typeMapping.get("name").toString()).isEqualTo("VARCHAR(300)");
    }

    @Test
    public void testAlterDropColumn() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .setPrimaryKeyNames("id")
                        .create();

        List<? extends Event> events = deserializer.deserialize(alterRecord(newTable));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DropColumnEvent.class);
        DropColumnEvent drop = (DropColumnEvent) events.get(0);
        assertThat(drop.tableId()).isEqualTo(TABLE_ID);
        assertThat(drop.getDroppedColumnNames()).containsExactly("name");
    }

    @Test
    public void testAlterWithoutObservedCreateIsSkipped() throws Exception {
        // The registry is empty (the CREATE of this table was not observed), so a single ALTER
        // change cannot be diffed and is skipped instead of emitting a conflicting CreateTableEvent.
        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("name", "VARCHAR", Types.VARCHAR, true, 2))
                        .setPrimaryKeyNames("id")
                        .create();

        assertThat(deserializer.deserialize(alterRecord(newTable))).isEmpty();
    }

    @Test
    public void testAlterWithLeadingOldSchemaIsDiffedWithoutObservedCreate() throws Exception {
        // The canal DDL handler carries the pre-change schema as a leading ALTER change, so an
        // ALTER of a table whose CREATE was never observed (snapshot tables announce their schema
        // via JDBC CreateTableEvents, bypassing the schema-change stream) can still be diffed into
        // column-level events: the leading change primes the registry, the trailing change is
        // diffed against it.
        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("name", "VARCHAR", Types.VARCHAR, true, 2, 255))
                        .addColumn(column("age", "INT", Types.INTEGER, true, 3))
                        .setPrimaryKeyNames("id")
                        .create();
        TableChanges changes = new TableChanges();
        changes.alter(BASE_TABLE);
        changes.alter(newTable);

        List<? extends Event> events = deserializer.deserialize(schemaChangeRecord(changes));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent add = (AddColumnEvent) events.get(0);
        assertThat(add.tableId()).isEqualTo(TABLE_ID);
        assertThat(add.getAddedColumns()).hasSize(1);
        assertThat(add.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("age");
    }

    @Test
    public void testRenameTable() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table renamedTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "vip_users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("name", "VARCHAR", Types.VARCHAR, true, 2, 255))
                        .setPrimaryKeyNames("id")
                        .create();

        List<? extends Event> events =
                deserializer.deserialize(
                        renameTableRecord(
                                renamedTable,
                                "test.vip_users",
                                "RENAME TABLE `test`.`users` TO `test`.`vip_users`"));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(RenameTableEvent.class);
        RenameTableEvent rename = (RenameTableEvent) events.get(0);
        assertThat(rename.getOldTableId()).isEqualTo(TABLE_ID);
        assertThat(rename.getNewTableId()).isEqualTo(TableId.tableId("test", "vip_users"));
        assertThat(rename.getSchema().getColumns()).hasSize(2);
        assertThat(rename.getSchema().primaryKeys()).containsExactly("id");
        assertThat(rename.getSql()).contains("RENAME TABLE");
    }

    @Test
    public void testAlterRenameColumn() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table newTable =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .addColumn(column("nickname", "VARCHAR", Types.VARCHAR, true, 2, 255))
                        .setPrimaryKeyNames("id")
                        .create();

        List<? extends Event> events = deserializer.deserialize(alterRecord(newTable));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(RenameColumnEvent.class);
        RenameColumnEvent rename = (RenameColumnEvent) events.get(0);
        assertThat(rename.tableId()).isEqualTo(TABLE_ID);
        assertThat(rename.getNameMapping()).containsEntry("name", "nickname");
    }

    @Test
    public void testDropTable() throws Exception {
        deserializer.deserialize(createRecord(BASE_TABLE));

        Table tableForDrop =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId("test", null, "users"))
                        .addColumn(column("id", "BIGINT", Types.BIGINT, false, 1))
                        .setPrimaryKeyNames("id")
                        .create();
        TableChanges changes = new TableChanges();
        changes.drop(tableForDrop);

        assertThat(deserializer.deserialize(schemaChangeRecord(changes))).isEmpty();
    }

    @Test
    public void testSchemaChangeDisabled() throws Exception {
        KafkaJsonEventDeserializer deserializerWithoutSchemaChange =
                new KafkaJsonEventDeserializer(DebeziumChangelogMode.ALL, false);

        assertThat(deserializerWithoutSchemaChange.deserialize(createRecord(BASE_TABLE)))
                .isEmpty();
    }

    @Test
    public void testInsertDataChange() throws Exception {
        Schema afterSchema =
                SchemaBuilder.struct()
                        .name("test.users.Value")
                        .field("id", Schema.INT64_SCHEMA)
                        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("test.users.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.users.Envelope")
                        .field("before", SchemaBuilder.struct().name("test.users.Value").optional())
                        .field("after", afterSchema)
                        .field("source", sourceSchema)
                        .field("op", Schema.STRING_SCHEMA)
                        .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                        .build();

        Struct value =
                new Struct(valueSchema)
                        .put("after", new Struct(afterSchema).put("id", 1L).put("name", "flink"))
                        .put(
                                "source",
                                new Struct(sourceSchema).put("db", "test").put("table", "users"))
                        .put("op", "c")
                        .put("ts_ms", 0L);
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "test",
                        null,
                        null,
                        valueSchema,
                        value);

        List<? extends Event> events = deserializer.deserialize(record);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChange = (DataChangeEvent) events.get(0);
        assertThat(dataChange.tableId()).isEqualTo(TABLE_ID);
        assertThat(dataChange.op()).isEqualTo(OperationType.INSERT);
    }

    // ----------------------------------------------------------------------------------------

    private static SourceRecord createRecord(Table table) {
        TableChanges changes = new TableChanges();
        changes.create(table);
        return schemaChangeRecord(changes);
    }

    private static SourceRecord alterRecord(Table table) {
        TableChanges changes = new TableChanges();
        changes.alter(table);
        return schemaChangeRecord(changes);
    }

    /**
     * Builds a schema-change record for a {@code RENAME TABLE}: the schema of the renamed table is
     * carried as a single CREATE table change, and the canal DDL handler attaches the custom {@code
     * tableChangeType}/{@code newTableId} fields to the history record.
     */
    private static SourceRecord renameTableRecord(Table table, String newTableId, String sql) {
        try {
            TableChanges changes = new TableChanges();
            changes.create(table);
            Array array = new FlinkJsonTableChangeSerializer().serialize(changes);
            Document historyDoc =
                    Document.create()
                            .set(HistoryRecord.Fields.TABLE_CHANGES, array)
                            .set(
                                    KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE,
                                    KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE)
                            .set(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID, newTableId)
                            .set(HistoryRecord.Fields.DDL_STATEMENTS, sql);
            String historyRecordStr = DocumentWriter.defaultWriter().write(historyDoc);

            Schema keySchema =
                    SchemaBuilder.struct()
                            .name(KafkaJsonEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                            .build();
            Schema sourceSchema =
                    SchemaBuilder.struct()
                            .field("db", Schema.STRING_SCHEMA)
                            .field("table", Schema.STRING_SCHEMA)
                            .build();
            Schema valueSchema =
                    SchemaBuilder.struct()
                            .field("source", sourceSchema)
                            .field("historyRecord", Schema.STRING_SCHEMA)
                            .build();
            Struct value =
                    new Struct(valueSchema)
                            .put(
                                    "source",
                                    new Struct(sourceSchema).put("db", "test").put("table", "users"))
                            .put("historyRecord", historyRecordStr);
            return new SourceRecord(
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    "test",
                    keySchema,
                    new Struct(keySchema),
                    valueSchema,
                    value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SourceRecord schemaChangeRecord(TableChanges changes) {
        try {
            Array array = new FlinkJsonTableChangeSerializer().serialize(changes);
            Document historyDoc = Document.create().set(HistoryRecord.Fields.TABLE_CHANGES, array);
            String historyRecordStr = DocumentWriter.defaultWriter().write(historyDoc);

            Schema keySchema =
                    SchemaBuilder.struct()
                            .name(KafkaJsonEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                            .build();
            Schema sourceSchema =
                    SchemaBuilder.struct()
                            .field("db", Schema.STRING_SCHEMA)
                            .field("table", Schema.STRING_SCHEMA)
                            .build();
            Schema valueSchema =
                    SchemaBuilder.struct()
                            .field("source", sourceSchema)
                            .field("historyRecord", Schema.STRING_SCHEMA)
                            .build();
            Struct value =
                    new Struct(valueSchema)
                            .put(
                                    "source",
                                    new Struct(sourceSchema).put("db", "test").put("table", "users"))
                            .put("historyRecord", historyRecordStr);
            return new SourceRecord(
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    "test",
                    keySchema,
                    new Struct(keySchema),
                    valueSchema,
                    value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Column column(
            String name, String type, int jdbcType, boolean optional, int position) {
        return column(name, type, jdbcType, optional, position, 0);
    }

    private static Column column(
            String name,
            String type,
            int jdbcType,
            boolean optional,
            int position,
            int length) {
        ColumnEditor editor =
                Column.editor()
                        .name(name)
                        .type(type)
                        .jdbcType(jdbcType)
                        .optional(optional)
                        .position(position);
        if (length > 0) {
            editor.length(length);
        }
        return editor.create();
    }
}
