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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AddColumnEvent.ColumnWithPosition;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Builds Doris DDL statements from the connector's schema-change events.
 *
 * <p>Tables are created in the UNIQUE model when the source schema declares a primary key (the
 * primary key doubles as the distribution key), or the DUPLICATE model otherwise. The distribution
 * uses {@code BUCKETS AUTO} so Doris picks the bucket count. Deletes are written by the sink in the
 * same StreamLoad batches through the {@code __DORIS_DELETE_SIGN__} marker column (declared via the
 * {@code hidden_columns} header), which the UNIQUE model accepts without any extra table property
 * ({@code enable_batch_delete_by_default}, the legacy batch-delete switch, is rejected by Doris 2.x
 * and is therefore not emitted).
 *
 * <p>Each event maps to a list of single-statement DDL strings (a multi-column event produces one
 * {@code ALTER TABLE} per column), which the {@link
 * org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisMetadataApplier} executes in
 * order. Type mapping mirrors the released pipeline-doris {@code DorisMetadataApplier}: all
 * timestamp kinds become {@code DATETIMEV2} with precision clamped to {@code [0, 6]}, and the
 * complex types (ARRAY/MAP/ROW) become {@code STRING} holding the JSON text produced by the row
 * converter.
 *
 * <p>{@code VARCHAR} lengths are mapped from characters (the unit of a MySQL/TiDB {@code VARCHAR})
 * to bytes (the unit Doris measures them in): a source {@code VARCHAR(n)} becomes {@code
 * VARCHAR(3n)} because {@code utf8mb4} stores at most three bytes per character, clamped to Doris's
 * {@value #DORIS_VARCHAR_MAX_BYTES}-byte maximum. {@code CHAR} counts characters in both engines
 * and is left unchanged.
 */
public class DorisDdlBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisDdlBuilder.class);

    /** Doris measures {@code VARCHAR(n)} in bytes; {@value} is its upper bound. */
    static final int DORIS_VARCHAR_MAX_BYTES = 65533;

    /** Upper bound of bytes a {@code utf8mb4} character can occupy. */
    private static final int BYTES_PER_UTF8_CHAR = 3;

    private final DorisDataSinkOptions options;

    public DorisDdlBuilder(DorisDataSinkOptions options) {
        this.options = options;
    }

    public List<String> buildCreateTableSql(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema schema = event.getSchema();
        StringJoiner columns = new StringJoiner(", ");
        List<String> primaryKeys = schema.primaryKeys();
        HashSet<String> pkSet = new LinkedHashSet<>(primaryKeys);
        List<Column> orderedColumns = new ArrayList<>();
        for (String pk : primaryKeys) {
            schema.getColumn(pk)
                    .filter(Column::isPhysical) // metadata columns are virtual and have no
                    // storage in Doris
                    .ifPresent(orderedColumns::add);
        }
        for (Column column : schema.getColumns()) {
            if (column.isPhysical() && !pkSet.contains(column.getName())) {
                orderedColumns.add(column);
            }
        }
        for (Column column : orderedColumns) {
            columns.add(
                    quote(column.getName())
                            + " "
                            + convertDataType(column.getType())
                            + commentSql(column.getComment()));
        }
        StringBuilder sql =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(qualified(tableId))
                        .append(" (")
                        .append(columns)
                        .append(")");
        if (schema.comment() != null && !schema.comment().isEmpty()) {
            sql.append(" COMMENT '").append(escapeSql(schema.comment())).append("'");
        }
        if (primaryKeys.isEmpty()) {
            String distributeKey = firstPhysicalColumn(schema);
            sql.append(" DUPLICATE KEY(")
                    .append(quote(distributeKey))
                    .append(") DISTRIBUTED BY HASH(")
                    .append(quote(distributeKey));
        } else {
            sql.append(" UNIQUE KEY(")
                    .append(quoteColumns(primaryKeys))
                    .append(") DISTRIBUTED BY HASH(")
                    .append(quoteColumns(primaryKeys));
        }
        int tableBuckets = options.getTableBuckets();
        if (tableBuckets > 0) {
            sql.append(") BUCKETS ").append(tableBuckets);
        } else {
            sql.append(") BUCKETS AUTO");
        }
        if (options.getTableProperties() != null) {
            sql.append(" PROPERTIES (")
                    .append(buildTableProperties(options.getTableProperties()))
                    .append(")");
        }
        return singleton(sql.toString());
    }

    private String buildTableProperties(Map<String, String> tableProperties) {
        return tableProperties.entrySet().stream()
                .map(
                        entry ->
                                quoteProperty(entry.getKey())
                                        + " = "
                                        + quoteProperty(entry.getValue()))
                .collect(Collectors.joining(", "));
    }

    public List<String> buildAddColumnSql(AddColumnEvent event) {
        TableId tableId = event.tableId();
        List<String> sqls = new ArrayList<>();
        for (ColumnWithPosition col : event.getAddedColumns()) {
            Column column = col.getAddColumn();
            sqls.add(
                    "ALTER TABLE "
                            + qualified(tableId)
                            + " ADD COLUMN "
                            + quote(column.getName())
                            + " "
                            + convertDataType(column.getType())
                            + commentSql(column.getComment()));
        }
        return sqls;
    }

    public List<String> buildDropColumnSql(DropColumnEvent event) {
        TableId tableId = event.tableId();
        List<String> sqls = new ArrayList<>();
        for (String column : event.getDroppedColumnNames()) {
            sqls.add("ALTER TABLE " + qualified(tableId) + " DROP COLUMN " + quote(column));
        }
        return sqls;
    }

    public List<String> buildRenameColumnSql(RenameColumnEvent event) {
        TableId tableId = event.tableId();
        List<String> sqls = new ArrayList<>();
        for (Map.Entry<String, String> entry : event.getNameMapping().entrySet()) {
            sqls.add(
                    "ALTER TABLE "
                            + qualified(tableId)
                            + " RENAME COLUMN "
                            + quote(entry.getKey())
                            + " "
                            + quote(entry.getValue()));
        }
        return sqls;
    }

    public List<String> buildAlterColumnTypeSql(AlterColumnTypeEvent event) {
        return buildAlterColumnTypeSql(event, Optional.empty());
    }

    /**
     * Builds the {@code MODIFY COLUMN} statements for an {@code AlterColumnTypeEvent}, optionally
     * against the {@code oldSchema} the column had before the change.
     *
     * <p>When the old type is known and the change shrinks a {@code CHAR}/{@code VARCHAR} column,
     * the statement is skipped with a warning: Doris cannot reduce the length of such a column
     * while MySQL/TiDB can, and a committed source DDL already guarantees the existing rows fit the
     * smaller length — keeping the wider Doris column accepts all subsequent data. All other type
     * changes (growth, cross-type, an unknown old type) are emitted as-is.
     */
    public List<String> buildAlterColumnTypeSql(
            AlterColumnTypeEvent event, Optional<Schema> oldSchema) {
        TableId tableId = event.tableId();
        List<String> sqls = new ArrayList<>();
        for (Map.Entry<String, DataType> entry : event.getTypeMapping().entrySet()) {
            String column = entry.getKey();
            DataType newType = entry.getValue();
            Optional<DataType> oldType =
                    oldSchema.flatMap(schema -> schema.getColumn(column)).map(Column::getType);
            if (isLengthReduction(oldType.orElse(null), newType)) {
                LOG.warn(
                        "Skipping ALTER TABLE {} MODIFY COLUMN `{}` from {} to {}: Doris does not "
                                + "support shrinking CHAR/VARCHAR length (MySQL/TiDB allows it); the "
                                + "wider Doris column keeps accepting the data.",
                        qualified(tableId),
                        column,
                        oldType.get(),
                        newType);
                continue;
            }
            sqls.add(
                    "ALTER TABLE "
                            + qualified(tableId)
                            + " MODIFY COLUMN "
                            + quote(column)
                            + " "
                            + convertDataType(newType));
        }
        return sqls;
    }

    public List<String> buildRenameTableSql(RenameTableEvent event) {
        // Doris renames a table within its database: ALTER TABLE db.old RENAME new
        return singleton(
                "ALTER TABLE "
                        + qualified(event.getOldTableId())
                        + " RENAME "
                        + quote(options.mapTable(event.getNewTableId())));
    }

    public List<String> buildDropTableSql(DropTableEvent event) {
        return singleton("DROP TABLE IF EXISTS " + qualified(event.tableId()));
    }

    public List<String> buildTruncateTableSql(TruncateTableEvent event) {
        return singleton("TRUNCATE TABLE " + qualified(event.tableId()));
    }

    public List<String> buildAlterTableCommentSql(AlterTableCommentEvent event) {
        return singleton(
                "ALTER TABLE "
                        + qualified(event.tableId())
                        + " COMMENT '"
                        + escapeSql(event.getComment())
                        + "'");
    }

    public List<String> buildAlterColumnCommentSql(AlterColumnCommentEvent event) {
        TableId tableId = event.tableId();
        List<String> sqls = new ArrayList<>();
        for (Map.Entry<String, String> entry : event.getCommentMapping().entrySet()) {
            sqls.add(
                    "ALTER TABLE "
                            + qualified(tableId)
                            + " MODIFY COLUMN "
                            + quote(entry.getKey())
                            + " COMMENT '"
                            + escapeSql(entry.getValue())
                            + "'");
        }
        return sqls;
    }

    /**
     * Maps a CDC {@link DataType} onto a Doris column type. Timestamps become {@code DATETIMEV2}
     * with precision clamped to the range Doris supports; ARRAY/MAP/ROW become {@code STRING} that
     * stores the JSON text rendered by {@code DorisRowConverter}.
     */
    private String convertDataType(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
                return "CHAR(" + ((CharType) type).getLength() + ")";
            case VARCHAR:
                // Doris VARCHAR(n) counts bytes; a MySQL/TiDB VARCHAR is counted in characters.
                return "VARCHAR(" + dorisVarcharLength(((VarCharType) type).getLength()) + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL(" + getPrecision(type) + ", " + getScale(type) + ")";
            case DATE:
                return "DATE";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return "DATETIMEV2(" + clampTimestampPrecision(getPrecision(type)) + ")";
            case ARRAY:
            case MAP:
            case ROW:
                return "STRING";
            default:
                throw new UnsupportedOperationException("Unsupported type for Doris DDL: " + type);
        }
    }

    /**
     * Maps a source {@code VARCHAR} length in characters onto Doris's byte budget: three bytes per
     * {@code utf8mb4} character, clamped to Doris's {@value #DORIS_VARCHAR_MAX_BYTES}-byte maximum.
     */
    private static int dorisVarcharLength(int sourceChars) {
        return (int) Math.min((long) sourceChars * BYTES_PER_UTF8_CHAR, DORIS_VARCHAR_MAX_BYTES);
    }

    /**
     * Returns the capacity the type consumes in Doris (bytes for {@code VARCHAR} after the {@code
     * ×3} mapping, characters for {@code CHAR}), or {@code -1} for a type outside the shrinkable
     * family.
     */
    private static int dorisCapacity(@Nullable DataType type) {
        if (type == null) {
            return -1;
        }
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return dorisVarcharLength(((VarCharType) type).getLength());
            case CHAR:
                return ((CharType) type).getLength();
            default:
                return -1;
        }
    }

    /**
     * Returns whether changing a column from {@code oldType} to {@code newType} would shrink the
     * capacity Doris reserves for it — the operation Doris rejects. Only a same-family change
     * ({@code VARCHAR}→{@code VARCHAR} or {@code CHAR}→{@code CHAR}) with the mapped new capacity
     * strictly smaller counts; a cross-type change (a {@code CHAR}/{@code VARCHAR} conversion
     * included) or an unknown old type is never judged as a reduction.
     */
    private static boolean isLengthReduction(@Nullable DataType oldType, DataType newType) {
        if (oldType == null || oldType.getTypeRoot() != newType.getTypeRoot()) {
            return false;
        }
        int oldCapacity = dorisCapacity(oldType);
        int newCapacity = dorisCapacity(newType);
        return oldCapacity >= 0 && newCapacity >= 0 && newCapacity < oldCapacity;
    }

    private String qualified(TableId tableId) {
        return quote(options.mapDatabase(tableId)) + "." + quote(options.mapTable(tableId));
    }

    private static String firstPhysicalColumn(Schema schema) {
        for (Column column : schema.getColumns()) {
            if (column.isPhysical()) {
                return column.getName();
            }
        }
        throw new IllegalStateException("Schema has no physical columns: " + schema);
    }

    private static String quote(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    private static String quoteProperty(String property) {
        return "\"" + property + "\"";
    }

    private static String quoteColumns(List<String> columns) {
        return columns.stream().map(DorisDdlBuilder::quote).collect(Collectors.joining(", "));
    }

    private static String commentSql(String comment) {
        return comment == null || comment.isEmpty() ? "" : " COMMENT '" + escapeSql(comment) + "'";
    }

    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }

    private static int clampTimestampPrecision(int precision) {
        return Math.max(0, Math.min(precision, 6));
    }

    private static List<String> singleton(String sql) {
        List<String> sqls = new ArrayList<>(1);
        sqls.add(sql);
        return sqls;
    }
}
