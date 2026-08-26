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

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;

/**
 * Normalized metadata of a Debezium {@link Column} plus the canonical MySQL/TiDB type mapping
 * shared by the SQL layer (Flink table {@code DataType}) and the pipeline layer (CDC common {@link
 * DataType}).
 *
 * <p><b>Why a single shared mapping:</b> the two layers used to duplicate this switch in their own
 * {@code KafkaJsonTypeUtils}. Keeping one implementation here means a fix applies to both layers at
 * once.
 *
 * <p><b>Temporal precision recovery:</b> the {@code length} of a Debezium {@link Column} carries
 * two meanings depending on the schema source. The streaming message path ({@link
 * KafkaJsonTableUtils#buildColumn}) parses the DDL text, so {@code "time(3)"} yields the
 * fractional-seconds precision ({@code 3}) directly. The JDBC snapshot path reports the <em>display
 * width</em> instead &mdash; verified on MySQL 8.0.46 and TiDB: {@code DATETIME(6)} &rarr; {@code
 * 26}, {@code DATETIME(3)} &rarr; {@code 23}, {@code DATETIME} &rarr; {@code 19}, {@code
 * TIMESTAMP(6)} &rarr; {@code 26}, {@code TIME(6)} &rarr; {@code 15}. The two are unambiguous
 * because the fractional-seconds precision is at most {@code 6} while the smallest display width is
 * {@code 8} (TIME) / {@code 19} (DATETIME/TIMESTAMP); a length above the fractional-seconds maximum
 * is a display width and is converted back to the precision ({@code width - 9} / {@code width - 20}
 * above that floor). Numeric and string types are clamped to their CDC/Flink bounds for the same
 * reason.
 */
public final class KafkaJsonColumnMeta {

    private static final int MAX_TEMPORAL_PRECISION = 6; // MySQL / TiDB fractional-seconds max
    private static final int MAX_DECIMAL_PRECISION = 38; // Flink / CDC decimal max
    private static final int DECIMAL_FALLBACK_SCALE = 18;

    private final String baseName;
    private final boolean unsigned;
    private final int precision;
    private final int scale;
    private final String originalTypeName;

    private KafkaJsonColumnMeta(
            String baseName, boolean unsigned, int precision, int scale, String originalTypeName) {
        this.baseName = baseName;
        this.unsigned = unsigned;
        this.precision = precision;
        this.scale = scale;
        this.originalTypeName = originalTypeName;
    }

    /**
     * Builds the normalized metadata of a Debezium {@link Column}.
     *
     * <p>The type name is read from the JDBC metadata {@code TYPE_NAME} column. Different JDBC
     * drivers may return a plain type name (e.g. {@code INT}) or one with modifiers (e.g. {@code
     * INT UNSIGNED ZEROFILL}), so the type name is normalized to {@code <BASE TYPE>[ UNSIGNED][
     * ZEROFILL]} before mapping. The length/scale are clamped to the valid range of the base type.
     */
    public static KafkaJsonColumnMeta fromColumn(Column column) {
        String typeName = column.typeName();
        String normalized = typeName == null ? "" : typeName.toUpperCase().trim();
        boolean unsigned = normalized.contains("UNSIGNED") || normalized.contains("ZEROFILL");
        String baseName = normalized.split(" ")[0];

        int precision = column.length();
        int scale = column.scale().orElse(0);

        switch (baseName) {
            case "TIME":
                // The length has two meanings. The message path (KafkaJsonTableUtils#buildColumn)
                // parses the DDL text, so "time(3)" yields the fractional-seconds precision (3)
                // directly. The JDBC snapshot path reports the display width ("HH:MM:SS" = 8, or
                // 9 + fsp when the fraction is present), from which the precision is recovered.
                // The two are unambiguous: the FSP is at most 6, the display width at least 8.
                precision =
                        precision <= MAX_TEMPORAL_PRECISION
                                ? Math.max(0, precision)
                                : precision > 8
                                        ? Math.min(precision - 9, MAX_TEMPORAL_PRECISION)
                                        : 0;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                // Same dual semantics as TIME: the message path carries the DDL precision directly,
                // the JDBC path the display width ("YYYY-MM-DD HH:MM:SS" = 19, or 20 + fsp with a
                // fraction). Display widths are at least 19, the FSP at most 6.
                precision =
                        precision <= MAX_TEMPORAL_PRECISION
                                ? Math.max(0, precision)
                                : precision > 19
                                        ? Math.min(precision - 20, MAX_TEMPORAL_PRECISION)
                                        : 0;
                break;
            case "BIT":
            case "CHAR":
            case "VARCHAR":
            case "BINARY":
            case "VARBINARY":
                // BIT(0)/CHAR(0)/VARCHAR(0)/BINARY(0)/VARBINARY(0) are rejected by both Flink and
                // CDC; 1 is the smallest length that carries a value.
                precision = Math.max(1, precision);
                break;
            case "NUMERIC":
            case "FIXED":
            case "DECIMAL":
                if (precision <= 0 || precision > MAX_DECIMAL_PRECISION) {
                    // handle decimal without an explicit precision/scale
                    precision = MAX_DECIMAL_PRECISION;
                    scale = DECIMAL_FALLBACK_SCALE;
                } else {
                    // scale must not exceed the precision
                    scale = Math.min(Math.max(scale, 0), precision);
                }
                break;
            default:
                break;
        }

        return new KafkaJsonColumnMeta(baseName, unsigned, precision, scale, typeName);
    }

    /**
     * Returns the corresponding CDC common {@link DataType}, applying {@code nullable} as declared
     * by the source column.
     */
    public DataType toCdcDataType(boolean nullable) {
        DataType type = convertToCdcType();
        return nullable ? type : type.notNull();
    }

    private DataType convertToCdcType() {
        switch (baseName) {
            case "BIT":
                // bit(1) is commonly used to represent a boolean value
                return precision == 1 ? DataTypes.BOOLEAN() : DataTypes.BINARY((precision + 7) / 8);
            case "BOOL":
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                // MySQL has no boolean type, it uses tinyint(1) to represent boolean
                return precision == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case "SMALLINT":
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case "MEDIUMINT":
                return DataTypes.INT();
            case "INT":
            case "INTEGER":
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case "YEAR":
                return DataTypes.INT();
            case "BIGINT":
            case "SERIAL":
                return unsigned ? DataTypes.DECIMAL(20, 0) : DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "REAL":
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DOUBLE();
            case "NUMERIC":
            case "FIXED":
            case "DECIMAL":
                // precision/scale are clamped in fromColumn to (0, 38] / [0, precision]
                return DataTypes.DECIMAL(precision, scale);
            case "TIME":
                return DataTypes.TIME(precision);
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
                return DataTypes.TIMESTAMP(precision);
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP_LTZ(precision);
            case "CHAR":
                return DataTypes.CHAR(precision);
            case "VARCHAR":
                return DataTypes.VARCHAR(precision);
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "JSON":
            case "ENUM":
            case "GEOMETRY":
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
            case "GEOMETRYCOLLECTION":
            case "GEOMCOLLECTION":
            case "MULTIPOINT":
            case "MULTIPOLYGON":
            case "MULTILINESTRING":
                return DataTypes.STRING();
            case "BINARY":
                return DataTypes.BINARY(precision);
            case "VARBINARY":
                return DataTypes.VARBINARY(precision);
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return DataTypes.BYTES();
            case "SET":
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", originalTypeName));
        }
    }
}
