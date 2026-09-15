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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A schema change event announcing that one or more tables were renamed.
 *
 * <p>The released flink-cdc runtime has no {@code RENAME_TABLE} event type, so a table rename would
 * otherwise be indistinguishable from a {@code DROP}+{@code CREATE} pair. This event carries both
 * table ids of every rename so that a downstream that builds its own event handling can migrate
 * per-table state from the old table id to the new one.
 *
 * <p><b>One event carries a whole statement.</b> {@code RENAME TABLE a TO b, c TO d} renames
 * several tables in one atomic statement, and the pairs stay together here for two reasons: a
 * downstream must migrate them together, and a sink that has to choose between a plain {@code ALTER
 * TABLE ... RENAME} and a name swap (Doris) can only tell whether the names form a cycle — as in
 * the {@code a TO b, b TO a} swap idiom — when it sees all the pairs at once. Splitting them into
 * one event per pair would also deadlock: the released {@code SchemaOperator} processes one schema
 * change event at a time to completion, so a coordinator that waited for a set of events to arrive
 * before applying them would wait forever.
 *
 * <p>The pairs are ordered as the statement wrote them. {@link #tableId()} reports the first pair's
 * old table id, which is the id the released runtime uses to flush and to refresh its per-table
 * caches — a rename has no single table id of its own, and the first old id is the representative
 * the single-pair case already used.
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

    private static final long serialVersionUID = 2L;

    /** Every rename of the statement, in statement order; never empty. */
    private final List<TableRename> pairs;

    @Nullable private final String sql;

    public RenameTableEvent(TableId oldTableId, TableId newTableId, Schema schema) {
        this(oldTableId, newTableId, schema, null);
    }

    public RenameTableEvent(
            TableId oldTableId, TableId newTableId, Schema schema, @Nullable String sql) {
        this(Collections.singletonList(new TableRename(oldTableId, newTableId, schema)), sql);
    }

    public RenameTableEvent(List<TableRename> pairs, @Nullable String sql) {
        if (pairs == null || pairs.isEmpty()) {
            throw new IllegalArgumentException("A rename must carry at least one table pair.");
        }
        this.pairs = new ArrayList<>(pairs);
        this.sql = sql;
    }

    /** Returns every rename of the statement, in statement order. */
    public List<TableRename> getPairs() {
        return Collections.unmodifiableList(pairs);
    }

    /** Returns the id of the table before the rename (the first pair's, when there are several). */
    public TableId getOldTableId() {
        return pairs.get(0).getOldTableId();
    }

    /** Returns the id of the table after the rename (the first pair's, when there are several). */
    public TableId getNewTableId() {
        return pairs.get(0).getNewTableId();
    }

    /** Returns the schema of the renamed table (the first pair's, when there are several). */
    public Schema getSchema() {
        return pairs.get(0).getSchema();
    }

    /** Returns the raw DDL statement, or {@code null} if not available. */
    @Nullable
    public String getSql() {
        return sql;
    }

    @Override
    public TableId tableId() {
        return getOldTableId();
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
        return Objects.equals(pairs, that.pairs) && Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pairs, sql);
    }

    @Override
    public String toString() {
        return "RenameTableEvent{" + "pairs=" + pairs + ", sql='" + sql + '\'' + '}';
    }

    /**
     * One {@code old TO new} table rename: the two table ids and the schema of the renamed table
     * (which a rename does not change — only the id moves).
     *
     * <p>It is {@link Serializable} because the event travels through the released runtime's
     * coordination channel, which serializes the whole {@link SchemaChangeEvent} with plain Java
     * serialization ({@code CoordinationResponseUtils}): an element of the pair list that is not
     * serializable fails the response with {@code NotSerializableException} before it reaches the
     * coordinator, and the schema-change request then times out.
     */
    public static class TableRename implements Serializable {

        private static final long serialVersionUID = 1L;

        private final TableId oldTableId;
        private final TableId newTableId;
        private final Schema schema;

        public TableRename(TableId oldTableId, TableId newTableId, Schema schema) {
            this.oldTableId = oldTableId;
            this.newTableId = newTableId;
            this.schema = schema;
        }

        public TableId getOldTableId() {
            return oldTableId;
        }

        public TableId getNewTableId() {
            return newTableId;
        }

        public Schema getSchema() {
            return schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableRename)) {
                return false;
            }
            TableRename that = (TableRename) o;
            return Objects.equals(oldTableId, that.oldTableId)
                    && Objects.equals(newTableId, that.newTableId)
                    && Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(oldTableId, newTableId, schema);
        }

        @Override
        public String toString() {
            return oldTableId + " -> " + newTableId;
        }
    }
}
