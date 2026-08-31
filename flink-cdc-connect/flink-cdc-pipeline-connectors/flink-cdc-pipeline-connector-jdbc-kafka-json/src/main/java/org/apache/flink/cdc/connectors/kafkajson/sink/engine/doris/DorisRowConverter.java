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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.sink.converter.KafkaJsonRowConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Renders {@link RecordData} rows for the Doris StreamLoad JSON body.
 *
 * <p>Mirrors the released pipeline-doris {@code DorisRowConverter} mapping — numbers are passed
 * through, strings are unboxed, dates and timestamps are formatted ({@code yyyy-MM-dd} /
 * {@code yyyy-MM-dd HH:mm:ss.SSSSSS}), and time-zone-aware timestamps are shifted to the pipeline
 * zone. The complex types are serialized to a JSON string (matching the {@code STRING} column type
 * chosen by {@code DorisDdlBuilder}); DELETE semantics are added by the sink writer, not here.
 */
public class DorisRowConverter extends KafkaJsonRowConverter {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public DorisRowConverter(Schema schema, ZoneId pipelineZoneId) {
        super(schema, pipelineZoneId);
    }

    @Override
    protected SerializationConverter createExternalConverter(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                int precision = getPrecision(type);
                int scale = getScale(type);
                return (index, val) -> val.getDecimal(index, precision, scale).toBigDecimal();
            case TINYINT:
                return (index, val) -> val.getByte(index);
            case SMALLINT:
                return (index, val) -> val.getShort(index);
            case INTEGER:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) ->
                        LocalDate.ofEpochDay(val.getInt(index)).format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                return (index, val) ->
                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)
                                .format(TIME_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (index, val) ->
                        val.getTimestamp(index, getPrecision(type))
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (index, val) ->
                        ZonedDateTime.ofInstant(
                                        val.getLocalZonedTimestampData(
                                                        index, getPrecision(type))
                                                .toInstant(),
                                        pipelineZoneId)
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                return (index, val) ->
                        ZonedDateTime.ofInstant(
                                        val.getZonedTimestamp(index, getPrecision(type))
                                                .toInstant(),
                                        pipelineZoneId)
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case ARRAY:
                return (index, val) -> writeValueAsString(convertArray(val.getArray(index), type));
            case MAP:
                return (index, val) -> writeValueAsString(convertMap(val.getMap(index), type));
            case ROW:
                return (index, val) ->
                        writeValueAsString(
                                convertRow(
                                        val.getRow(index, getFieldCount(type)), (RowType) type));
            default:
                throw new UnsupportedOperationException("Unsupported type for Doris: " + type);
        }
    }

    private Object convertArray(ArrayData array, DataType type) {
        DataType elementType = ((ArrayType) type).getElementType();
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        List<Object> result = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            result.add(convertValue(elementGetter.getElementOrNull(array, i), elementType));
        }
        return result;
    }

    private Object convertMap(MapData map, DataType type) {
        DataType keyType = ((MapType) type).getKeyType();
        DataType valueType = ((MapType) type).getValueType();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        ArrayData keyArray = map.keyArray();
        ArrayData valueArray = map.valueArray();
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            Object key = convertValue(keyGetter.getElementOrNull(keyArray, i), keyType);
            Object value = convertValue(valueGetter.getElementOrNull(valueArray, i), valueType);
            // JSON object keys must be strings; Doris STRING columns hold the serialized text.
            result.put(key == null ? null : key.toString(), value);
        }
        return result;
    }

    private Object convertRow(RecordData row, RowType rowType) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            Object value =
                    RecordData.createFieldGetter(field.getType(), i).getFieldOrNull(row);
            result.put(field.getName(), convertValue(value, field.getType()));
        }
        return result;
    }

    /** Converts an already-extracted nested value into a JSON-ready object for the given type. */
    private Object convertValue(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return ((StringData) value).toString();
            case BOOLEAN:
                return value;
            case BINARY:
            case VARBINARY:
                return value;
            case DECIMAL:
                return ((DecimalData) value).toBigDecimal();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value;
            case DATE:
                return LocalDate.ofEpochDay(((Number) value).intValue())
                        .format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L)
                        .format(TIME_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampData) value).toLocalDateTime().format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ZonedDateTime.ofInstant(
                                ((LocalZonedTimestampData) value).toInstant(), pipelineZoneId)
                        .toLocalDateTime()
                        .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedDateTime.ofInstant(
                                ((ZonedTimestampData) value).toInstant(), pipelineZoneId)
                        .toLocalDateTime()
                        .format(DATE_TIME_FORMATTER);
            case ARRAY:
                return convertArray((ArrayData) value, type);
            case MAP:
                return convertMap((MapData) value, type);
            case ROW:
                return convertRow((RecordData) value, (RowType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type for Doris: " + type);
        }
    }

    private static String writeValueAsString(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize nested value for Doris: " + value, e);
        }
    }
}
