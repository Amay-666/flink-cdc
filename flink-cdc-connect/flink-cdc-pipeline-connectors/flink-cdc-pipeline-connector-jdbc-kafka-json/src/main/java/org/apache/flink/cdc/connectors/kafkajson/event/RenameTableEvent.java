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
 * A schema change event announcing that a table was renamed.
 *
 * <p>The released flink-cdc runtime has no {@code RENAME_TABLE} event type, so a table rename would
 * otherwise be indistinguishable from a {@code DROP}+{@code CREATE} pair. This event carries both
 * table ids so that a downstream that builds its own event handling can migrate per-table state
 * from the old table id to the new one.
 *
 * <p>Note: the released {@link SchemaChangeEventType} enum has no {@code RENAME_TABLE} value, so
 * {@link #getType()} cannot answer — it throws {@link UnsupportedOperationException} instead of
 * returning a placeholder, which would make an event the released runtime cannot express look like
 * a {@code CREATE_TABLE}. Every path this connector builds dispatches on the concrete class via
 * {@code instanceof} (the serialization stack does too), so {@link #getType()} is never consulted;
 * reaching it means code written for the released event set met this event.
 */
@PublicEvolving
public class RenameTableEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId oldTableId;
    private final TableId newTableId;
    private final Schema schema;
    @Nullable private final String sql;

    public RenameTableEvent(TableId oldTableId, TableId newTableId, Schema schema) {
        this(oldTableId, newTableId, schema, null);
    }

    public RenameTableEvent(
            TableId oldTableId, TableId newTableId, Schema schema, @Nullable String sql) {
        this.oldTableId = oldTableId;
        this.newTableId = newTableId;
        this.schema = schema;
        this.sql = sql;
    }

    /** Returns the id of the table before the rename (the table the change was announced on). */
    public TableId getOldTableId() {
        return oldTableId;
    }

    /** Returns the id of the table after the rename. */
    public TableId getNewTableId() {
        return newTableId;
    }

    /** Returns the schema of the renamed table. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns the raw DDL statement, or {@code null} if not available. */
    @Nullable
    public String getSql() {
        return sql;
    }

    @Override
    public TableId tableId() {
        return oldTableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        throw new UnsupportedOperationException(
                "RenameTableEvent is not supported by released flink-cdc runtime.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RenameTableEvent)) {
            return false;
        }
        RenameTableEvent that = (RenameTableEvent) o;
        return Objects.equals(oldTableId, that.oldTableId)
                && Objects.equals(newTableId, that.newTableId)
                && Objects.equals(schema, that.schema)
                && Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldTableId, newTableId, schema, sql);
    }

    @Override
    public String toString() {
        return "RenameTableEvent{"
                + "oldTableId="
                + oldTableId
                + ", newTableId="
                + newTableId
                + ", schema="
                + schema
                + ", sql='"
                + sql
                + '\''
                + '}';
    }
}
