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
import java.util.LinkedHashMap;
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
        // Group Commit: inject a physical BIGINT sequence column for UNIQUE-model tables.
        // Doris uses this column (declared via function_column.sequence_col in PROPERTIES) to
        // resolve ordering when Group Commit reorders batch commits. The writer populates it
        // with a monotonically increasing value per subtask.
        boolean injectSequence = options.isGroupCommitEnabled() && !primaryKeys.isEmpty();
        if (injectSequence) {
            columns.add(quote(options.getSequenceColumnName()) + " BIGINT");
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
        // Merge framework-injected Group Commit properties with user-supplied table properties.
        // Framework properties go in first; user's sink.table.properties can override any of them
        // (including function_column.sequence_col if the user wants a different column name — but
        // they must then ensure that column exists in the table).
        LinkedHashMap<String, String> allProps = new LinkedHashMap<>();
        if (injectSequence) {
            allProps.put("function_column.sequence_col", options.getSequenceColumnName());
            allProps.put(
                    "group_commit_interval_ms", String.valueOf(options.getGroupCommitIntervalMs()));
            allProps.put(
                    "group_commit_data_bytes", String.valueOf(options.getGroupCommitDataBytes()));
        }
        if (options.getTableProperties() != null) {
            allProps.putAll(options.getTableProperties());
        }
        if (!allProps.isEmpty()) {
            sql.append(" PROPERTIES (").append(buildTableProperties(allProps)).append(")");
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
     * <p>When the old type is known and the change shrinks a column's capacity — be it a {@code
     * CHAR}/{@code VARCHAR} length, an integer width ({@code BIGINT → INT}), a float width ({@code
     * DOUBLE → FLOAT}), a {@code DECIMAL} precision/scale, or a timestamp precision — the statement
     * is skipped with a warning: Doris cannot reduce these while MySQL/TiDB can, and a committed
     * source DDL already guarantees the existing rows fit the smaller type — keeping the wider
     * Doris column accepts all subsequent data. All other type changes (growth, cross-family, an
     * unknown old type) are emitted as-is.
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
            if (isSafeReduction(oldType.orElse(null), newType)) {
                LOG.warn(
                        "Skipping ALTER TABLE {} MODIFY COLUMN `{}` from {} to {}: this is a "
                                + "capacity reduction (Doris either rejects the narrowing or the "
                                + "wider column already accepts the narrower data); keeping the "
                                + "Doris column as-is.",
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
        // Doris parses "ALTER TABLE t COMMENT ..." as a syntax error (verified against Doris
        // 2.1.8); the table comment is changed with MODIFY COMMENT. Column comments use the
        // "MODIFY COLUMN c COMMENT ..." form in buildAlterColumnCommentSql, which Doris accepts
        // without repeating the column type.
        return singleton(
                "ALTER TABLE "
                        + qualified(event.tableId())
                        + " MODIFY COMMENT '"
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
     * with precision clamped to the range Doris supports; TIME also becomes {@code STRING} (Doris
     * has no TIME type, and the row converter renders {@code HH:mm:ss} text); ARRAY/MAP/ROW become
     * {@code STRING} that stores the JSON text rendered by {@code DorisRowConverter}.
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
            case TIME_WITHOUT_TIME_ZONE:
                // Doris has no TIME type. DorisRowConverter already renders the value as the
                // "HH:mm:ss" text of the row's JSON, so the column is created as the STRING that
                // holds that rendering — the same mapping the released Doris Flink connector
                // applies to Flink's TimeType. Without this case every table with a TIME column
                // fails at CREATE/ADD/MODIFY COLUMN time with "Unsupported type for Doris DDL".
                return "STRING";
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
     * capacity Doris reserves for it — an operation Doris either rejects or one where the wider old
     * column safely accepts all narrower new data, making the DDL unnecessary.
     *
     * <p>Recognised reductions:
     *
     * <ul>
     *   <li><b>String length shrinking</b> (same root): {@code VARCHAR(n₁) → VARCHAR(n₂)} where the
     *       mapped byte capacity n₂ < n₁; {@code CHAR(n₁) → CHAR(n₂)} where n₂ < n₁.
     *   <li><b>Integer narrowing</b> (cross-root within the integer family): {@code BIGINT → INT →
     *       SMALLINT → TINYINT}. The wider Doris integer column accepts all narrower data.
     *   <li><b>Float narrowing</b> (cross-root within the float family): {@code DOUBLE → FLOAT}.
     *   <li><b>Decimal precision/scale shrinking</b> (same root): {@code DECIMAL(p₁,s₁) →
     *       DECIMAL(p₂,s₂)} where the new integer-digit budget p₂ − s₂ and the new scale s₂ are
     *       both ≤ the old (at least one strict). The old column's value space is then a superset
     *       of the new one.
     *   <li><b>Timestamp precision shrinking</b> (same or cross-root within the timestamp family):
     *       the Doris-mapped precision is strictly smaller after clamping to {@code DATETIMEV2}'s
     *       {@code 6}-digit maximum. All three CDC timestamp roots map to the same Doris {@code
     *       DATETIMEV2(p)} type.
     * </ul>
     *
     * <p>Cross-family changes that are not pure narrowing (e.g. {@code VARCHAR → INT}, {@code
     * DECIMAL → BIGINT}, {@code FLOAT → INT}) are never judged as reductions: the semantic shift
     * means the old Doris column may not accept the new data.
     */
    private static boolean isSafeReduction(@Nullable DataType oldType, DataType newType) {
        if (oldType == null) {
            return false;
        }

        // 1. Same type root: compare capacity within the same type
        if (oldType.getTypeRoot() == newType.getTypeRoot()) {
            return isSameTypeReduction(oldType, newType);
        }

        // 2. Cross-root integer narrowing: BIGINT → INT → SMALLINT → TINYINT
        int oldIntWidth = integerByteWidth(oldType);
        int newIntWidth = integerByteWidth(newType);
        if (oldIntWidth > 0 && newIntWidth > 0) {
            return newIntWidth < oldIntWidth;
        }

        // 3. Cross-root float narrowing: DOUBLE → FLOAT
        int oldFloatWidth = floatByteWidth(oldType);
        int newFloatWidth = floatByteWidth(newType);
        if (oldFloatWidth > 0 && newFloatWidth > 0) {
            return newFloatWidth < oldFloatWidth;
        }

        // 4. Cross-root timestamp precision shrinking (all 3 roots map to DATETIMEV2(p))
        int oldTsCapacity = dorisTimestampCapacity(oldType);
        int newTsCapacity = dorisTimestampCapacity(newType);
        if (oldTsCapacity >= 0 && newTsCapacity >= 0) {
            return newTsCapacity < oldTsCapacity;
        }

        return false;
    }

    /**
     * Checks whether a same-root type change is a capacity reduction.
     *
     * <ul>
     *   <li>{@code VARCHAR}/{@code CHAR}: mapped capacity strictly smaller
     *   <li>{@code DECIMAL}: the new integer-digit budget {@code (p − s)} and the new scale are
     *       both ≤ the old (at least one strict) — equivalently the old column's value space is a
     *       superset of the new one
     *   <li>Timestamp roots: the Doris-mapped precision (clamped to {@code DATETIMEV2}'s {@code
     *       6}-digit maximum) strictly smaller
     * </ul>
     */
    private static boolean isSameTypeReduction(DataType oldType, DataType newType) {
        switch (oldType.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                int oldCap = dorisCapacity(oldType);
                int newCap = dorisCapacity(newType);
                return oldCap >= 0 && newCap >= 0 && newCap < oldCap;
            case DECIMAL:
                // Doris DECIMAL(p,s) stores (p − s) integer digits and s fraction digits; the old
                // column accepts all narrower data iff neither budget grows.
                int oldIntDigits = getPrecision(oldType) - getScale(oldType);
                int newIntDigits = getPrecision(newType) - getScale(newType);
                int oldScale = getScale(oldType);
                int newScale = getScale(newType);
                return newIntDigits <= oldIntDigits
                        && newScale <= oldScale
                        && (newIntDigits < oldIntDigits || newScale < oldScale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return dorisTimestampCapacity(newType) < dorisTimestampCapacity(oldType);
            default:
                return false;
        }
    }

    /** Returns the byte width of an integer type, or 0 if the type is not an integer. */
    private static int integerByteWidth(DataType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
                return 4;
            case BIGINT:
                return 8;
            default:
                return 0;
        }
    }

    /** Returns the byte width of a floating-point type, or 0 if the type is not a float. */
    private static int floatByteWidth(DataType type) {
        switch (type.getTypeRoot()) {
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            default:
                return 0;
        }
    }

    /**
     * Returns the capacity — in fractional-seconds digits — the type reserves in Doris, or -1 if
     * the type is not a timestamp. All three CDC timestamp roots map to the same Doris {@code
     * DATETIMEV2(p)} type and {@code p} is clamped to Doris's {@code 6}-digit maximum (see {@link
     * #clampTimestampPrecision}), so capacity comparisons use the clamped precision to stay
     * consistent with the column type {@code convertDataType} actually emits.
     */
    private static int dorisTimestampCapacity(DataType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return clampTimestampPrecision(getPrecision(type));
            default:
                return -1;
        }
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
        return "\"" + property.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String quoteColumns(List<String> columns) {
        return columns.stream().map(DorisDdlBuilder::quote).collect(Collectors.joining(", "));
    }

    private static String commentSql(String comment) {
        return comment == null || comment.isEmpty() ? "" : " COMMENT '" + escapeSql(comment) + "'";
    }

    /**
     * Escapes a value for a Doris string literal.
     *
     * <p>The backslash is escaped first, because Doris reads it as an escape character inside a
     * literal: a comment holding a Windows path, e.g. {@code C:\path\to}, was sent verbatim and
     * stored as {@code C:path<TAB>o} — the {@code \p} escape was dropped as unknown and {@code \t}
     * became a tab, silently corrupting the comment. Doubling the backslash makes the value
     * round-trip byte for byte (verified against Doris 2.1.8 with a {@code HEX()} comparison).
     * Doubling the quote then needs no further escaping, and is the form Doris parses back to a
     * single quote.
     */
    private static String escapeSql(String value) {
        return value.replace("\\", "\\\\").replace("'", "''");
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
