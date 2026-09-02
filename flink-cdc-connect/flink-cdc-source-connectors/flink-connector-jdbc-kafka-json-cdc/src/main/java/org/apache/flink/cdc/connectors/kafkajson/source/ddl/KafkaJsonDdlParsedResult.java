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

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * The result of parsing one canal DDL message: the affected table, the type of the schema change
 * and the table schemas before and after the change.
 *
 * <p>Unlike Debezium's {@code TableChanges.TableChangeType} — which only knows {@code
 * CREATE}/{@code ALTER}/{@code DROP} — the {@link KafkaJsonTableChangeType} also models {@code
 * RENAME_TABLE} and {@code RENAME_COLUMN}, so a rename is carried with both the old and the new
 * table id / schema instead of being flattened into a {@code DROP}+{@code CREATE} pair. Likewise a
 * truncate is carried with the preserved schema (the truncated table is not dropped).
 */
public class KafkaJsonDdlParsedResult {

    private final KafkaJsonTableChangeType type;
    /** The affected table; for a {@link KafkaJsonTableChangeType#RENAME_TABLE} the old table id. */
    private final TableId tableId;
    /**
     * The new table id of a {@link KafkaJsonTableChangeType#RENAME_TABLE}, otherwise {@code null}.
     */
    @Nullable private final TableId newTableId;
    /** The schema before the change, or {@code null} if unknown (e.g. {@code CREATE}). */
    @Nullable private final Table oldTable;
    /** The schema after the change, or {@code null} for a {@link KafkaJsonTableChangeType#DROP}. */
    @Nullable private final Table newTable;
    /**
     * The list of column-level changes for {@code ALTER} operations, or an empty list if not
     * applicable (e.g., when parsed by the Debezium ANTLR parser).
     */
    private final List<ColumnChangeInfo> columnChanges;

    public KafkaJsonDdlParsedResult(
            KafkaJsonTableChangeType type,
            TableId tableId,
            @Nullable TableId newTableId,
            @Nullable Table oldTable,
            @Nullable Table newTable) {
        this(type, tableId, newTableId, oldTable, newTable, Collections.emptyList());
    }

    public KafkaJsonDdlParsedResult(
            KafkaJsonTableChangeType type,
            TableId tableId,
            @Nullable TableId newTableId,
            @Nullable Table oldTable,
            @Nullable Table newTable,
            List<ColumnChangeInfo> columnChanges) {
        this.type = type;
        this.tableId = tableId;
        this.newTableId = newTableId;
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.columnChanges = columnChanges != null ? columnChanges : Collections.emptyList();
    }

    public static KafkaJsonDdlParsedResult create(TableId tableId, Table newTable) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.CREATE, tableId, null, null, newTable);
    }

    public static KafkaJsonDdlParsedResult alter(
            TableId tableId, @Nullable Table oldTable, Table newTable) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.ALTER, tableId, null, oldTable, newTable);
    }

    /**
     * Returns an ALTER result with column change details.
     *
     * @param columnChanges the list of column-level changes.
     */
    public static KafkaJsonDdlParsedResult alter(
            TableId tableId,
            @Nullable Table oldTable,
            Table newTable,
            List<ColumnChangeInfo> columnChanges) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.ALTER, tableId, null, oldTable, newTable, columnChanges);
    }

    public static KafkaJsonDdlParsedResult drop(TableId tableId, @Nullable Table oldTable) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.DROP, tableId, null, oldTable, null);
    }

    public static KafkaJsonDdlParsedResult renameTable(
            TableId oldTableId,
            TableId newTableId,
            @Nullable Table oldTable,
            @Nullable Table newTable) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.RENAME_TABLE, oldTableId, newTableId, oldTable, newTable);
    }

    public static KafkaJsonDdlParsedResult renameColumn(
            TableId tableId, Table oldTable, Table newTable) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.RENAME_COLUMN, tableId, null, oldTable, newTable);
    }

    /** Returns a truncate result: the schema is preserved, so {@code oldTable == newTable}. */
    public static KafkaJsonDdlParsedResult truncate(TableId tableId, Table table) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.TRUNCATE, tableId, tableId, table, table);
    }

    public KafkaJsonTableChangeType getType() {
        return type;
    }

    public TableId getTableId() {
        return tableId;
    }

    /**
     * Returns the new table id of a {@link KafkaJsonTableChangeType#RENAME_TABLE}, else {@code
     * null}.
     */
    @Nullable
    public TableId getNewTableId() {
        return newTableId;
    }

    /** Returns the schema before the change, or {@code null} if unknown. */
    @Nullable
    public Table getOldTable() {
        return oldTable;
    }

    /**
     * Returns the schema after the change, or {@code null} for a {@link
     * KafkaJsonTableChangeType#DROP}.
     */
    @Nullable
    public Table getNewTable() {
        return newTable;
    }

    /**
     * Returns the list of column-level changes for {@code ALTER} operations, or an empty list if
     * not applicable.
     */
    public List<ColumnChangeInfo> getColumnChanges() {
        return columnChanges;
    }

    /**
     * Returns whether the change from {@code oldTable} to {@code newTable} is a pure column rename:
     * the two tables have the same number of columns, at every position the column type is
     * unchanged and at exactly one position the column name differs. Used by the DDL parsers to
     * classify a single-column rename as {@link KafkaJsonTableChangeType#RENAME_COLUMN}.
     */
    public static boolean isPureColumnRename(Table oldTable, Table newTable) {
        List<Column> oldColumns = oldTable.columns();
        List<Column> newColumns = newTable.columns();
        if (oldColumns.size() != newColumns.size()) {
            return false;
        }
        int renameCount = 0;
        for (int i = 0; i < oldColumns.size(); i++) {
            Column oldColumn = oldColumns.get(i);
            Column newColumn = newColumns.get(i);
            if (!sameColumnType(oldColumn, newColumn)) {
                return false;
            }
            if (!oldColumn.name().equals(newColumn.name())) {
                renameCount++;
            }
        }
        return renameCount == 1;
    }

    /**
     * Type-only equality: jdbc type, length, scale and type name (optionality and position
     * excluded).
     */
    private static boolean sameColumnType(Column a, Column b) {
        return a.jdbcType() == b.jdbcType()
                && a.length() == b.length()
                && a.scale() == b.scale()
                && a.typeName().equals(b.typeName());
    }

    public static KafkaJsonDdlParsedResult alterColumnType(
            TableId tableId,
            @Nullable Table oldTable,
            Table newTable,
            List<ColumnChangeInfo> columnChanges) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.ALTER_COLUMN_TYPE,
                tableId,
                null,
                oldTable,
                newTable,
                columnChanges);
    }

    public static KafkaJsonDdlParsedResult alterColumnComment(
            TableId tableId,
            @Nullable Table oldTable,
            Table newTable,
            List<ColumnChangeInfo> columnChanges) {
        return new KafkaJsonDdlParsedResult(
                KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT,
                tableId,
                null,
                oldTable,
                newTable,
                columnChanges);
    }

    /**
     * Returns whether the change from {@code oldTable} to {@code newTable} is a pure column type
     * change: the two tables have the same columns (same names and count), no comment changes and
     * exactly one column whose type (jdbc type/length/scale/type name) differs. Position and
     * optionality differences are treated as ignorable and do not disqualify the classification.
     */
    public static boolean isPureColumnTypeChange(Table oldTable, Table newTable) {
        return isPureColumnAspectChange(oldTable, newTable, true, false);
    }

    /**
     * Returns whether the change from {@code oldTable} to {@code newTable} is a pure column comment
     * change: the two tables have the same columns (same names and count), no type changes and
     * exactly one column whose comment differs. Position and optionality differences are treated as
     * ignorable and do not disqualify the classification.
     */
    public static boolean isPureColumnCommentChange(Table oldTable, Table newTable) {
        return isPureColumnAspectChange(oldTable, newTable, false, true);
    }

    private static boolean isPureColumnAspectChange(
            Table oldTable, Table newTable, boolean checkType, boolean checkComment) {
        List<Column> oldColumns = oldTable.columns();
        List<Column> newColumns = newTable.columns();
        if (oldColumns.size() != newColumns.size()) {
            return false;
        }
        int changedCount = 0;
        for (Column oldColumn : oldColumns) {
            Column newColumn = findColumn(newColumns, oldColumn.name());
            if (newColumn == null) {
                return false; // a renamed/added/dropped column disqualifies a pure column change
            }
            boolean typeDiffers = !sameColumnType(oldColumn, newColumn);
            boolean commentDiffers = !equalsSafe(oldColumn.comment(), newColumn.comment());
            if ((checkType && typeDiffers) || (checkComment && commentDiffers)) {
                changedCount++;
            } else if (typeDiffers || commentDiffers) {
                return false; // the other aspect changed somewhere: a mixed alter, not a pure
                // subtype
            }
        }
        return changedCount == 1;
    }

    /**
     * Returns whether the change from {@code oldTable} to {@code newTable} carries no material
     * column change: the same columns under the same names with the same type and comment,
     * differing only in aspects that are ignored for schema-change events (position, optionality,
     * default value, charset, auto-increment, generated flag). The DDL parsers use this to skip an
     * {@code ALTER} that only reorders columns or toggles nullability.
     */
    public static boolean hasOnlyIgnorableColumnChanges(Table oldTable, Table newTable) {
        List<Column> oldColumns = oldTable.columns();
        List<Column> newColumns = newTable.columns();
        if (oldColumns.size() != newColumns.size()) {
            return false;
        }
        for (Column oldColumn : oldColumns) {
            Column newColumn = findColumn(newColumns, oldColumn.name());
            if (newColumn == null
                    || !sameColumnType(oldColumn, newColumn)
                    || !equalsSafe(oldColumn.comment(), newColumn.comment())) {
                return false;
            }
            // position, optionality, default value, charset, auto-increment, generated -> ignorable
        }
        return true;
    }

    private static Column findColumn(List<Column> columns, String name) {
        for (Column column : columns) {
            if (column.name().equals(name)) {
                return column;
            }
        }
        return null;
    }

    private static boolean equalsSafe(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }
}
