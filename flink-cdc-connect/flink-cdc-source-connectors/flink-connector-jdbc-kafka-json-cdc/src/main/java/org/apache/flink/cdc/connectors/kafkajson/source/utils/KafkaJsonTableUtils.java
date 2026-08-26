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

import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonFlatMessage;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds a Debezium {@link Table} from the type metadata that canal attaches to each flatMessage
 * ({@code mysqlType} / {@code sqlType} / {@code pkNames}).
 *
 * <p>This is the fallback path of the schema resolution: when a table is not (yet) known to {@code
 * KafkaJsonSchema} — e.g. a table created after the snapshot discovery, or a stream-only setup
 * without JDBC credentials — the column types are reconstructed from the message itself. The
 * canonical path still prefers the JDBC schema from {@code KafkaJsonSchema}.
 */
public class KafkaJsonTableUtils {

    /** {@code int(11)}, {@code varchar(255)}, {@code decimal(10,2)}, {@code timestamp(3)}, ... */
    private static final Pattern PARAM_PATTERN =
            Pattern.compile(
                    "^\\s*([a-z0-9]+)\\s*(?:\\((.*?)\\))?\\s*(unsigned)?\\s*(zerofill)?\\s*$");

    private static final Set<String> STRING_TYPES =
            new LinkedHashSet<>(
                    Arrays.asList("char", "varchar", "tinytext", "text", "mediumtext", "longtext"));

    private static final Set<String> BINARY_TYPES =
            new LinkedHashSet<>(Arrays.asList("binary", "varbinary"));

    private static final Set<String> BLOB_TYPES =
            new LinkedHashSet<>(Arrays.asList("blob", "tinyblob", "mediumblob", "longblob"));

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
     * Builds a {@link Table} for the message. The column order is taken from {@code data} when
     * available (the first row), otherwise from {@code mysqlType}.
     *
     * @param message the canal flat message
     * @return the reconstructed table, or {@code null} if the message carries no column metadata at
     *     all
     */
    public static Table buildTable(KafkaJsonFlatMessage message) {
        TableId tableId =
                new TableId(
                        message.getDatabase() == null ? "" : message.getDatabase(),
                        null,
                        message.getTable());
        List<String> columns = columnNames(message);
        if (columns.isEmpty()) {
            return null;
        }
        Map<String, String> mysqlType =
                message.getMysqlType() == null ? Collections.emptyMap() : message.getMysqlType();
        List<String> pkNames =
                message.getPkNames() == null ? Collections.emptyList() : message.getPkNames();

        TableEditor table = Table.editor().tableId(tableId);
        int position = 0;
        for (String columnName : columns) {
            ColumnEditor editor = buildColumn(columnName, mysqlType.get(columnName), position + 1);
            table.addColumn(editor.create());
            position++;
        }
        if (!pkNames.isEmpty()) {
            table.setPrimaryKeyNames(pkNames);
        }
        return table.create();
    }

    private static List<String> columnNames(KafkaJsonFlatMessage message) {
        if (message.getData() != null && !message.getData().isEmpty()) {
            return new ArrayList<>(message.getData().get(0).keySet());
        }
        if (message.getMysqlType() != null) {
            return new ArrayList<>(message.getMysqlType().keySet());
        }
        return Collections.emptyList();
    }

    /**
     * Builds a {@link ColumnEditor} from a column name and its MySQL type expression.
     *
     * <p>Shared with the DDL parsers ({@code KafkaJsonDruidDdlParser}), which reconstruct the type
     * expression (e.g. {@code varchar(255) unsigned}) from their AST so that a DDL-derived column
     * and a message-derived column of the same type are built identically.
     */
    public static ColumnEditor buildColumn(String name, String mysqlType, int position) {
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
                enumValues = parseEnumValues(params);
                break;
            case "set":
                typeName = "SET";
                jdbcType = Types.VARCHAR;
                enumValues = parseEnumValues(params);
                break;
            default:
                if (STRING_TYPES.contains(base)) {
                    typeName = "VARCHAR";
                    jdbcType = Types.VARCHAR;
                } else if (BINARY_TYPES.contains(base)) {
                    typeName = base.equals("binary") ? "BINARY" : "VARBINARY";
                    jdbcType = base.equals("binary") ? Types.BINARY : Types.VARBINARY;
                } else if (BLOB_TYPES.contains(base)) {
                    typeName = "BLOB";
                    jdbcType = Types.BLOB;
                } else {
                    throw new FlinkRuntimeException("Unsupported mysqlType: " + mysqlType);
                }
        }
        return new ParsedType(base, typeName, jdbcType, length, scale, enumValues);
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

    private static List<String> parseEnumValues(String params) {
        if (params == null) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<>();
        Matcher matcher = Pattern.compile("'((?:[^']|'')*)'").matcher(params);
        while (matcher.find()) {
            values.add(matcher.group(1).replace("''", "'"));
        }
        return values;
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
