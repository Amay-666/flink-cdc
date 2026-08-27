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

package org.apache.flink.cdc.connectors.kafkajson.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A schema change event announcing that a table's comment was changed.
 *
 * <p>The released flink-cdc runtime has no {@code ALTER_TABLE_COMMENT} event type, so a comment
 * change would otherwise be indistinguishable from a plain {@code ALTER_TABLE}. This event carries
 * the post-change schema, the new table comment and the raw DDL so a downstream can react to the
 * comment change explicitly.
 */
@PublicEvolving
public class AlterTableCommentEvent implements SchemaChangeEvent {
    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final Schema schema;
    @Nullable
    private final String sql;
    private final String comment;

    public AlterTableCommentEvent(TableId tableId, Schema schema, @Nullable String sql, String comment) {
        this.tableId = tableId;
        this.schema = schema;
        this.sql = sql;
        this.comment = comment;
    }

    /** Returns the id of the truncated table. */
    @Override
    public TableId tableId() {
        return tableId;
    }

    /** Returns the schema of the truncated table. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns the raw DDL statement, or {@code null} if not available. */
    @Nullable
    public String getSql() {
        return sql;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public SchemaChangeEventType getType() {
        throw new UnsupportedOperationException("AlterTableCommentEvent is not supported by released flink-cdc runtime.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterTableCommentEvent)) {
            return false;
        }
        AlterTableCommentEvent that = (AlterTableCommentEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(schema, that.schema)
                && Objects.equals(sql, that.sql)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema, sql, comment);
    }

    @Override
    public String toString() {
        return "AlterTableCommentEvent{"
                + "tableId="
                + tableId
                + ", schema="
                + schema
                + ", sql='"
                + sql
                + '\''
                + ", comment='"
                + comment
                + '\''
                + '}';
    }
}
