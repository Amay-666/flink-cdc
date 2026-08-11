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

package org.apache.flink.cdc.connectors.kafkajson.utils;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;

/**
 * A utility class for converting MySQL types to the {@link
 * org.apache.flink.cdc.common.types.DataType} used by the pipeline layer.
 *
 * <p>This mirrors the mapping of the SQL-layer {@code KafkaJsonTypeUtils} but produces the common
 * {@code DataType} instead of the Flink table {@code DataType}.
 */
@Internal
public class KafkaJsonTypeUtils {

    // The base (modifier-free) MySQL type names returned by JDBC drivers.
    private static final String BIT = "BIT";
    private static final String BOOL = "BOOL";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String INT = "INT";
    private static final String INTEGER = "INTEGER";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String REAL = "REAL";
    private static final String FLOAT = "FLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String NUMERIC = "NUMERIC";
    private static final String FIXED = "FIXED";
    private static final String DECIMAL = "DECIMAL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String POINT = "POINT";
    private static final String LINESTRING = "LINESTRING";
    private static final String POLYGON = "POLYGON";
    private static final String GEOMCOLLECTION = "GEOMCOLLECTION";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    private static final String MULTIPOINT = "MULTIPOINT";
    private static final String MULTIPOLYGON = "MULTIPOLYGON";
    private static final String MULTILINESTRING = "MULTILINESTRING";

    /** Returns a corresponding common {@link DataType} from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    private static DataType convertFromColumn(Column column) {
        String typeName = column.typeName();
        String normalized = typeName == null ? "" : typeName.toUpperCase().trim();
        boolean unsigned =
                normalized.contains("UNSIGNED") || normalized.contains("ZEROFILL");
        String baseName = normalized.split(" ")[0];

        int precision = column.length();
        int scale = column.scale().orElse(0);

        switch (baseName) {
            case BIT:
                // bit(1) is commonly used to represent a boolean value
                return precision == 1 ? DataTypes.BOOLEAN() : DataTypes.BINARY((precision + 7) / 8);
            case BOOL:
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                // MySQL has no boolean type, it uses tinyint(1) to represent boolean
                return precision == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case SMALLINT:
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case MEDIUMINT:
                return DataTypes.INT();
            case INT:
            case INTEGER:
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case YEAR:
                return DataTypes.INT();
            case BIGINT:
            case SERIAL:
                return unsigned ? DataTypes.DECIMAL(20, 0) : DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case REAL:
            case DOUBLE:
            case DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case NUMERIC:
            case FIXED:
            case DECIMAL:
                // handle decimal without explicit precision and scale
                if (precision > 0 && precision <= 38) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(38, 18);
            case TIME:
                return DataTypes.TIME(precision);
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP(precision);
            case TIMESTAMP:
                return DataTypes.TIMESTAMP_LTZ(precision);
            case CHAR:
                return DataTypes.CHAR(precision);
            case VARCHAR:
                return DataTypes.VARCHAR(precision);
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case GEOMETRYCOLLECTION:
            case GEOMCOLLECTION:
            case MULTIPOINT:
            case MULTIPOLYGON:
            case MULTILINESTRING:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(precision);
            case VARBINARY:
                return DataTypes.VARBINARY(precision);
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return DataTypes.BYTES();
            case SET:
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", typeName));
        }
    }

    private KafkaJsonTypeUtils() {}
}
