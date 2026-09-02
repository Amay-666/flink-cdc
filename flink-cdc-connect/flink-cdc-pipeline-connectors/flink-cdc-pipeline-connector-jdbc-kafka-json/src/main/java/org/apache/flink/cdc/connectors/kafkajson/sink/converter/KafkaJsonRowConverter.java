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

package org.apache.flink.cdc.connectors.kafkajson.sink.converter;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base converter that turns a {@link RecordData} row into a map of column name to a
 * dialect-specific, JSON-ready value.
 *
 * <p>Mirrors the {@code SerializationConverter} pattern of the released pipeline-doris {@code
 * DorisRowConverter}: every column gets a converter created from its {@link DataType} and {@link
 * #convert(RecordData, Schema)} applies them in column order. Dialects implement {@link
 * #createExternalConverter(DataType)} to decide how each CDC data type is rendered on the wire; the
 * null handling and the conversion loop are shared here.
 *
 * <p>Instances are <em>not</em> bound to a single schema version: {@link #convert(RecordData,
 * Schema)} lazily rebuilds the per-column converters whenever the passed schema differs from the
 * one the current converters were built from, so one converter instance follows schema evolution of
 * a table.
 */
public abstract class KafkaJsonRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Runtime converter for one column: extracts the field at {@code index} and renders it. */
    @FunctionalInterface
    public interface SerializationConverter extends Serializable {
        Object serialize(int index, RecordData field);
    }

    /** The zone id used to render time-zone-aware values (date/time/datetime, ...). */
    protected final ZoneId pipelineZoneId;

    /** The schema the current {@link #fieldConverters} were built from. */
    private volatile Schema schema;

    private volatile List<String> fieldNames;
    private volatile List<SerializationConverter> fieldConverters;

    protected KafkaJsonRowConverter(Schema schema, ZoneId pipelineZoneId) {
        this.pipelineZoneId = pipelineZoneId;
        rebuild(schema);
    }

    /**
     * Converts one row according to the given schema, rebuilding the per-column converters first if
     * the schema changed.
     */
    public Map<String, Object> convert(RecordData recordData, Schema schema) {
        if (!schema.equals(this.schema)) {
            rebuild(schema);
        }
        List<String> names = this.fieldNames;
        List<SerializationConverter> converters = this.fieldConverters;
        Map<String, Object> row = new LinkedHashMap<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            row.put(names.get(i), converters.get(i).serialize(i, recordData));
        }
        return row;
    }

    /** Creates a nullable wrapper around a converter for a column of the given type. */
    protected SerializationConverter createNullableExternalConverter(DataType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type));
    }

    /** Wraps a converter so that null fields are rendered as {@code null} without delegating. */
    protected static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            }
            return serializationConverter.serialize(index, val);
        };
    }

    /**
     * Creates the converter for a non-null value of the given type. Dialects implement the mapping
     * from CDC {@link DataType}s to their target representation.
     */
    protected abstract SerializationConverter createExternalConverter(DataType type);

    private void rebuild(Schema schema) {
        List<String> names = schema.getColumnNames();
        List<SerializationConverter> converters = new ArrayList<>(names.size());
        for (String name : names) {
            DataType columnType =
                    schema.getColumn(name)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Missing column \"%s\" in schema %s",
                                                            name, schema)))
                            .getType();
            converters.add(createNullableExternalConverter(columnType));
        }
        this.schema = schema;
        this.fieldNames = names;
        this.fieldConverters = converters;
    }
}
