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

package org.apache.flink.cdc.connectors.kafkajson.source.utils;

import org.apache.flink.cdc.connectors.kafkajson.source.type.MySqlEnumSetLabels;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;

import javax.annotation.Nullable;

import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds Debezium {@link Column}s from MySQL type expressions ({@code bigint(20) unsigned}, {@code
 * varchar(255)}, {@code enum('a','b')}, ...).
 *
 * <p>The DDL parsers reconstruct the type expression from their AST and pass it here, so a table
 * built from a DDL is identical to the JDBC one for every type the connector supports. A canal
 * flatMessage's own {@code mysqlType} is deliberately not a source of table schemas: it names MySQL
 * types only, without the length or scale a Debezium {@link Column} needs, so a schema built from
 * it would silently describe the wrong columns.
 */
public class KafkaJsonTableUtils {

    /** {@code int(11)}, {@code varchar(255)}, {@code decimal(10,2)}, {@code timestamp(3)}, ... */
    private static final Pattern PARAM_PATTERN =
            Pattern.compile(
                    "^\\s*([a-z0-9]+)\\s*(?:\\((.*?)\\))?\\s*(unsigned)?\\s*(zerofill)?\\s*$");

    private static final Set<String> GEOMETRY_TYPES =
            new LinkedHashSet<>(
                    Arrays.asList(
                            "geometry",
                            "point",
                            "linestring",
                            "polygon",
                            "multipoint",
                            "multilinestring",
                            "multipolygon",
                            "geometrycollection",
                            "geomcollection"));

    private KafkaJsonTableUtils() {}

    /**
     * Builds a {@link ColumnEditor} from a column name and its MySQL type expression.
     *
     * <p>Shared with the DDL parsers ({@code KafkaJsonDruidDdlParser}), which reconstruct the type
     * expression (e.g. {@code varchar(255) unsigned}) from their AST so that a DDL-derived column
     * and a message-derived column of the same type are built identically.
     */
    public static ColumnEditor buildColumn(String name, String mysqlType, int position) {
        return buildColumn(name, mysqlType, position, null);
    }

    /**
     * Builds a {@link ColumnEditor} from a column name, its MySQL type expression and comment.
     *
     * <p>The DDL parsers pass the {@code COMMENT '...'} clause of a column definition here so that
     * a comment change on a {@code MODIFY}/{@code CHANGE} column can be detected;
     */
    public static ColumnEditor buildColumn(
            String name, String mysqlType, int position, @Nullable String comment) {
        ParsedType parsed = parseType(mysqlType);
        if (GEOMETRY_TYPES.contains(parsed.base)) {
            throw new FlinkRuntimeException(
                    "Unsupported geometry column '" + name + "' with type '" + mysqlType + "'");
        }
        ColumnEditor editor =
                Column.editor()
                        .name(name)
                        .position(position)
                        .type(parsed.typeName, mysqlType)
                        .jdbcType(parsed.jdbcType)
                        .length(parsed.length)
                        .scale(parsed.scale)
                        .optional(true);
        if (comment != null) {
            editor.comment(comment);
        }
        if (parsed.enumValues != null) {
            editor.enumValues(parsed.enumValues);
        }
        if (parsed.jdbcType == Types.DECIMAL && parsed.length == 0) {
            // Debezium requires a non-zero precision for decimal
            editor.length(36);
        }
        return editor;
    }

    /** Parses a MySQL type expression like {@code "bigint(20) unsigned zerofill"}. */
    private static ParsedType parseType(String mysqlType) {
        if (mysqlType == null) {
            return new ParsedType("varchar", "VARCHAR", Types.VARCHAR, 255, null, null);
        }
        Matcher matcher = PARAM_PATTERN.matcher(mysqlType.toLowerCase(Locale.ROOT));
        if (!matcher.matches()) {
            throw new FlinkRuntimeException("Unsupported mysqlType expression: " + mysqlType);
        }
        String base = matcher.group(1);
        String params = matcher.group(2);
        boolean unsigned = matcher.group(3) != null;
        int[] lenScale = parseParams(base, params);
        int length = lenScale[0];
        Integer scale = lenScale[1] >= 0 ? lenScale[1] : null;

        String typeName;
        int jdbcType;
        List<String> enumValues = null;
        switch (base) {
            case "tinyint":
                // matches MySqlAntlrDdlParser: TINYINT maps to Types.SMALLINT (schema INT16)
                typeName = unsigned ? "TINYINT UNSIGNED" : "TINYINT";
                jdbcType = Types.SMALLINT;
                break;
            case "smallint":
                typeName = unsigned ? "SMALLINT UNSIGNED" : "SMALLINT";
                jdbcType = Types.SMALLINT;
                break;
            case "mediumint":
                typeName = unsigned ? "MEDIUMINT UNSIGNED" : "MEDIUMINT";
                jdbcType = Types.INTEGER;
                break;
            case "int":
            case "integer":
                typeName = unsigned ? "INT UNSIGNED" : "INT";
                jdbcType = Types.INTEGER;
                break;
            case "bigint":
                typeName = unsigned ? "BIGINT UNSIGNED" : "BIGINT";
                jdbcType = Types.BIGINT;
                break;
            case "float":
                typeName = "FLOAT";
                jdbcType = Types.FLOAT;
                break;
            case "double":
            case "real": // MySQL synonym of DOUBLE (REAL_AS_FLOAT mode is not modeled)
                typeName = "DOUBLE";
                jdbcType = Types.DOUBLE;
                break;
            case "decimal":
            case "numeric":
                typeName = "DECIMAL";
                jdbcType = Types.DECIMAL;
                break;
            case "bit":
                typeName = "BIT";
                jdbcType = Types.BIT;
                break;
            case "bool":
            case "boolean":
                typeName = "BOOLEAN";
                jdbcType = Types.BOOLEAN;
                break;
            case "date":
                typeName = "DATE";
                jdbcType = Types.DATE;
                break;
            case "time":
                typeName = "TIME";
                jdbcType = Types.TIME;
                break;
            case "datetime":
                typeName = "DATETIME";
                jdbcType = Types.TIMESTAMP;
                break;
            case "timestamp":
                typeName = "TIMESTAMP";
                jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
                break;
            case "year":
                typeName = "YEAR";
                jdbcType = Types.DATE;
                break;
            case "json":
                typeName = "JSON";
                jdbcType = Types.LONGVARCHAR;
                break;
            case "enum":
                typeName = "ENUM";
                jdbcType = Types.VARCHAR;
                enumValues = MySqlEnumSetLabels.parse(mysqlType);
                break;
            case "set":
                typeName = "SET";
                jdbcType = Types.VARCHAR;
                enumValues = MySqlEnumSetLabels.parse(mysqlType);
                break;
            case "char":
                // mirror the JDBC metadata path: TYPE_NAME "CHAR", jdbc type CHAR — collapsing
                // CHAR into VARCHAR here would diverge from a JDBC-discovered column (whose
                // KafkaJsonColumnMeta mapping is CHAR(precision)) and surface as a phantom
                // type change whenever the two schemas are diffed
                typeName = "CHAR";
                jdbcType = Types.CHAR;
                break;
            case "varchar":
                typeName = "VARCHAR";
                jdbcType = Types.VARCHAR;
                break;
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                // mirror the JDBC metadata path: TYPE_NAME carries the flavor and COLUMN_SIZE
                // the flavor's byte limit, so KafkaJsonColumnMeta maps the column to STRING
                // exactly like a JDBC-discovered one. Collapsing to VARCHAR here instead made
                // the missing length clamp to VARCHAR(1) in the sink's DDL — a one-character
                // column for a TEXT field. The (rare, ignored by MySQL) TEXT(M) display hint
                // is not honored, matching COLUMN_SIZE semantics.
                typeName = base.toUpperCase(Locale.ROOT);
                jdbcType = Types.LONGVARCHAR;
                length = textBlobLength(base);
                break;
            case "binary":
                typeName = "BINARY";
                jdbcType = Types.BINARY;
                break;
            case "varbinary":
                typeName = "VARBINARY";
                jdbcType = Types.VARBINARY;
                break;
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                // same parity as the TEXT family: the flavor TYPE_NAME, LONGVARBINARY and the
                // byte limit; KafkaJsonColumnMeta maps every flavor to BYTES either way
                typeName = base.toUpperCase(Locale.ROOT);
                jdbcType = Types.LONGVARBINARY;
                length = textBlobLength(base);
                break;
            default:
                throw new FlinkRuntimeException("Unsupported mysqlType: " + mysqlType);
        }
        if (jdbcType == Types.FLOAT || jdbcType == Types.DOUBLE || jdbcType == Types.REAL) {
            // Approximate numerics carry no scale: the parsed display scale ("float(12,0)" —
            // TiCDC renders the display width) is garbage (MySQL deprecated FLOAT(M,D)), and a
            // present scale suppresses the single-precision FLOAT32 schema branch in
            // MySqlValueConverters. That mismatch makes the record carry a DOUBLE layout
            // while the column type stays FLOAT, and reading 4 bytes from the 8-byte slot
            // silently corrupts the value to 0.0.
            scale = null;
        }
        return new ParsedType(base, typeName, jdbcType, length, scale, enumValues);
    }

    /**
     * The byte limit of the TEXT/BLOB flavor families, matching what the JDBC driver reports in
     * {@code COLUMN_SIZE}: {@code tinytext}/{@code tinyblob} 2^8-1, {@code text}/{@code blob}
     * 2^16-1, {@code mediumtext}/{@code mediumblob} 2^24-1 and {@code longtext}/{@code longblob}
     * 2^32-1.
     */
    private static int textBlobLength(String base) {
        switch (base) {
            case "tinytext":
            case "tinyblob":
                return 255;
            case "text":
            case "blob":
                return 65535;
            case "mediumtext":
            case "mediumblob":
                return 16777215;
            case "longtext":
            case "longblob":
            default:
                return 2147483647;
        }
    }

    private static int[] parseParams(String base, String params) {
        if (params == null) {
            return new int[] {0, -1};
        }
        if (base.equals("enum") || base.equals("set")) {
            return new int[] {0, -1};
        }
        String[] parts = params.split(",");
        try {
            int length = Integer.parseInt(parts[0].trim());
            int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : -1;
            return new int[] {length, scale};
        } catch (NumberFormatException e) {
            return new int[] {0, -1};
        }
    }

    /** Whether the (upper-cased) type name denotes an unsigned integer type. */
    public static boolean isUnsigned(String upperCaseTypeName) {
        return upperCaseTypeName != null
                && (upperCaseTypeName.startsWith("TINYINT UNSIGNED")
                        || upperCaseTypeName.startsWith("SMALLINT UNSIGNED")
                        || upperCaseTypeName.startsWith("MEDIUMINT UNSIGNED")
                        || upperCaseTypeName.startsWith("INT UNSIGNED")
                        || upperCaseTypeName.startsWith("BIGINT UNSIGNED"));
    }

    private static final class ParsedType {
        final String base;
        final String typeName;
        final int jdbcType;
        final int length;
        final Integer scale;
        final List<String> enumValues;

        ParsedType(
                String base,
                String typeName,
                int jdbcType,
                int length,
                Integer scale,
                List<String> enumValues) {
            this.base = base;
            this.typeName = typeName;
            this.jdbcType = jdbcType;
            this.length = length;
            this.scale = scale;
            this.enumValues = enumValues;
        }
    }
}
