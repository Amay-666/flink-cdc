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

package org.apache.flink.cdc.connectors.kafkajson.unit.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.utils.SchemaChangeUtil;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import org.junit.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the minimal schema-change inference of {@link SchemaChangeUtil}. */
public class SchemaChangeUtilTest {

    private static final TableId TABLE_ID = TableId.tableId("test", "users");

    @Test
    public void testCommentOnlyChange() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, null)),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, "nickname")));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnCommentEvent.class);
        AlterColumnCommentEvent alter = (AlterColumnCommentEvent) events.get(0);
        assertThat(alter.tableId()).isEqualTo(TABLE_ID);
        assertThat(alter.getCommentMapping()).containsEntry("name", "nickname");
    }

    @Test
    public void testCommentRemoved() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, "nickname")),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, null)));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnCommentEvent.class);
        AlterColumnCommentEvent alter = (AlterColumnCommentEvent) events.get(0);
        assertThat(alter.getCommentMapping()).containsKey("name");
        assertThat(alter.getCommentMapping().get("name")).isNull();
    }

    @Test
    public void testNullabilityOnlyChangeIgnored() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, null)),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, false, 255, null)));

        assertThat(events).isEmpty();
    }

    @Test
    public void testRenameWithCommentChange() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, null)),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("nickname", "VARCHAR", Types.VARCHAR, true, 255, "nk")));

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(RenameColumnEvent.class);
        RenameColumnEvent rename = (RenameColumnEvent) events.get(0);
        assertThat(rename.getNameMapping()).containsEntry("name", "nickname");
        assertThat(events.get(1)).isInstanceOf(AlterColumnCommentEvent.class);
        AlterColumnCommentEvent alter = (AlterColumnCommentEvent) events.get(1);
        assertThat(alter.getCommentMapping()).containsEntry("nickname", "nk");
    }

    @Test
    public void testTypeAndCommentChange() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, null)),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 300, "c")));

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent type = (AlterColumnTypeEvent) events.get(0);
        assertThat(type.getTypeMapping()).containsOnlyKeys("name");
        assertThat(type.getTypeMapping().get("name").toString()).isEqualTo("VARCHAR(300)");
        assertThat(events.get(1)).isInstanceOf(AlterColumnCommentEvent.class);
        AlterColumnCommentEvent alter = (AlterColumnCommentEvent) events.get(1);
        assertThat(alter.getCommentMapping()).containsEntry("name", "c");
    }

    @Test
    public void testAddedColumnCarriesComment() {
        List<SchemaChangeEvent> events =
                SchemaChangeUtil.inferMinimalSchemaChanges(
                        TABLE_ID,
                        Arrays.asList(col("id", "BIGINT", Types.BIGINT, false, 0, null)),
                        Arrays.asList(
                                col("id", "BIGINT", Types.BIGINT, false, 0, null),
                                col("name", "VARCHAR", Types.VARCHAR, true, 255, "n")));

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent add = (AddColumnEvent) events.get(0);
        assertThat(add.getAddedColumns()).hasSize(1);
        assertThat(add.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("name");
        assertThat(add.getAddedColumns().get(0).getAddColumn().getComment()).isEqualTo("n");
    }

    private static Column col(
            String name, String type, int jdbcType, boolean optional, int length, String comment) {
        ColumnEditor editor =
                Column.editor().name(name).type(type).jdbcType(jdbcType).optional(optional);
        if (length > 0) {
            editor.length(length);
        }
        if (comment != null) {
            editor.comment(comment);
        }
        return editor.create();
    }
}
