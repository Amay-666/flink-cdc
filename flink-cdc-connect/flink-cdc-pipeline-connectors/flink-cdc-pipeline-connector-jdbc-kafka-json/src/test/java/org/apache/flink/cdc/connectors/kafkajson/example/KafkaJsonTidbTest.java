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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.factory.KafkaJsonDataSourceFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonEventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manual end-to-end harness for a TiDB deployment: a {@code main} method rather than tests, driven
 * by the environment described in {@code e2e/env.yaml} (see {@link E2eConfig}). The repository only
 * carries {@code e2e/env.yaml.template}, so no real connection details are committed.
 */
public class KafkaJsonTidbTest {

    /** Format DATE type data. */
    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonTidbTest.class);

    public static void main(String[] args) throws Exception {
        // The endpoints and credentials come from e2e/env.yaml (see E2eConfig); the repository only
        // carries e2e/env.yaml.template, so no real connection details are committed.
        E2eConfig e2e = E2eConfig.get();
        String database = e2e.getString("mysql.database");

        Configuration factoryConfiguration = new Configuration();

        factoryConfiguration.set(KafkaJsonDataSourceOptions.HOSTNAME, e2e.getString("mysql.host"));
        factoryConfiguration.set(KafkaJsonDataSourceOptions.PORT, e2e.getInt("mysql.port"));
        factoryConfiguration.set(KafkaJsonDataSourceOptions.USERNAME, e2e.getString("mysql.user"));
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.PASSWORD, e2e.getString("mysql.password"));
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.TABLES,
                database + ".test_schema_change3|test_schema_change4");
        factoryConfiguration.set(KafkaJsonDataSourceOptions.DATABASE_TYPE, "tidb");
        factoryConfiguration.set(KafkaJsonDataSourceOptions.EVENT_TIME, "tidb_tso");
        factoryConfiguration.set(KafkaJsonDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, 1000);
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 5000);
        factoryConfiguration.set(KafkaJsonDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED, true);
        factoryConfiguration.set(KafkaJsonDataSourceOptions.SCHEMA_CHANGE_ENABLED, true);
        factoryConfiguration.set(KafkaJsonDataSourceOptions.SCAN_STARTUP_MODE, "initial");
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.KAFKA_BOOTSTRAP_SERVERS, e2e.getString("kafka.servers"));
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.KAFKA_GROUP_ID, e2e.getString("kafka.group"));
        factoryConfiguration.set(
                KafkaJsonDataSourceOptions.SCAN_KAFKA_TOPICS, e2e.getString("kafka.topic"));
        factoryConfiguration.set(KafkaJsonDataSourceOptions.KAFKA_STARTUP_MODE, "latest");

        Configuration pipelineConfiguration = new Configuration();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        FactoryHelper.DefaultContext context =
                new FactoryHelper.DefaultContext(
                        factoryConfiguration, pipelineConfiguration, classLoader);

        KafkaJsonDataSourceFactory kafkaJsonDataSourceFactory = new KafkaJsonDataSourceFactory();
        DataSource kafkaJsonDataSource = kafkaJsonDataSourceFactory.createDataSource(context);
        FlinkSourceProvider eventSourceProvider =
                (FlinkSourceProvider) kafkaJsonDataSource.getEventSourceProvider();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Source<Event, ?, ?> source = eventSourceProvider.getSource();
        if (source instanceof KafkaJsonEventSource) {
            env.enableCheckpointing(5000);
            env.setParallelism(1);
            KafkaJsonEventSource incrementalSource = (KafkaJsonEventSource) source;
            DataStream<Event> eventStream =
                    env.fromSource(
                            incrementalSource,
                            WatermarkStrategy.noWatermarks(),
                            "KafkaJsonEventSource");

            SingleOutputStreamOperator<Object> sinkRow =
                    eventStream
                            .keyBy(event -> 1)
                            .map(
                                    new RichMapFunction<Event, Object>() {
                                        private volatile MapState<TableId, Schema> schemas;

                                        @Override
                                        public void open(
                                                org.apache.flink.configuration.Configuration
                                                        parameters)
                                                throws Exception {
                                            MapStateDescriptor<TableId, Schema> stateProperties =
                                                    new MapStateDescriptor<>(
                                                            "schemas",
                                                            TypeInformation.of(
                                                                    new TypeHint<TableId>() {}),
                                                            TypeInformation.of(
                                                                    new TypeHint<Schema>() {}));
                                            schemas =
                                                    getRuntimeContext()
                                                            .getMapState(stateProperties);
                                        }

                                        @Override
                                        public Object map(Event event) throws Exception {
                                            if (event instanceof DataChangeEvent) {
                                                DataChangeEvent dataChangeEvent =
                                                        (DataChangeEvent) event;
                                                OperationType op = dataChangeEvent.op();
                                                int arity = dataChangeEvent.after().getArity();
                                                List<DataType> dataTypes =
                                                        schemas.get(dataChangeEvent.tableId())
                                                                .getColumnDataTypes();
                                                switch (op) {
                                                    case INSERT:
                                                        List<Object> values = new ArrayList<>();
                                                        for (int i = 0; i < arity; i++) {
                                                            SerializationConverter converter =
                                                                    createNullableExternalConverter(
                                                                            dataTypes.get(i),
                                                                            ZoneId.systemDefault());
                                                            Object value =
                                                                    converter.serialize(
                                                                            i,
                                                                            dataChangeEvent
                                                                                    .after());
                                                            //
                                                            //  RecordData.FieldGetter fieldGetter =
                                                            // RecordData.createFieldGetter(dataTypes.get(i), i);
                                                            //
                                                            //  Object value =
                                                            // fieldGetter.getFieldOrNull(dataChangeEvent.after());
                                                            values.add(value);
                                                        }
                                                        LOG.debug("INSERT data={}", values);
                                                        return "TableId:"
                                                                + dataChangeEvent.tableId()
                                                                + ",values:"
                                                                + values;
                                                    case UPDATE:
                                                        List<Object> beforeValues =
                                                                new ArrayList<>();
                                                        List<Object> afterValues =
                                                                new ArrayList<>();
                                                        for (int i = 0; i < arity; i++) {
                                                            SerializationConverter converter =
                                                                    createNullableExternalConverter(
                                                                            dataTypes.get(i),
                                                                            ZoneId.systemDefault());
                                                            Object beforeValue =
                                                                    converter.serialize(
                                                                            i,
                                                                            dataChangeEvent
                                                                                    .before());
                                                            Object afterValue =
                                                                    converter.serialize(
                                                                            i,
                                                                            dataChangeEvent
                                                                                    .after());
                                                            beforeValues.add(beforeValue);
                                                            afterValues.add(afterValue);
                                                        }
                                                        LOG.debug(
                                                                "UPDATE beforeValues={}, afterValues={}",
                                                                beforeValues,
                                                                afterValues);
                                                        return "TableId:"
                                                                + dataChangeEvent.tableId()
                                                                + ",beforeValues:"
                                                                + beforeValues
                                                                + ",afterValues:"
                                                                + afterValues;
                                                    case DELETE:
                                                        List<Object> deleteValues =
                                                                new ArrayList<>();
                                                        for (int i = 0; i < arity; i++) {
                                                            SerializationConverter converter =
                                                                    createNullableExternalConverter(
                                                                            dataTypes.get(i),
                                                                            ZoneId.systemDefault());
                                                            Object value =
                                                                    converter.serialize(
                                                                            i,
                                                                            dataChangeEvent
                                                                                    .before());
                                                            deleteValues.add(value);
                                                        }
                                                        LOG.debug("DELETE data={}", deleteValues);
                                                        return deleteValues;
                                                }
                                            } else if (event instanceof SchemaChangeEvent) {
                                                if (event instanceof CreateTableEvent) {
                                                    CreateTableEvent createTableEvent =
                                                            (CreateTableEvent) event;
                                                    Schema schema =
                                                            schemas.get(createTableEvent.tableId());
                                                    if (schema == null) {
                                                        schemas.put(
                                                                createTableEvent.tableId(),
                                                                createTableEvent.getSchema());
                                                    } else {
                                                        if (!schema.equals(
                                                                createTableEvent.getSchema())) {
                                                            schemas.put(
                                                                    createTableEvent.tableId(),
                                                                    createTableEvent.getSchema());
                                                            LOG.debug(
                                                                    "CreateTableEvent={}",
                                                                    createTableEvent);
                                                            return event;
                                                        } else {
                                                            return "";
                                                        }
                                                    }
                                                } else if (event instanceof TruncateTableEvent) {
                                                    LOG.debug("TruncateTableEvent={}", event);
                                                    return event;
                                                } else if (event instanceof RenameTableEvent) {
                                                    LOG.debug("RenameTableEvent={}", event);
                                                    TableId oldTableId =
                                                            ((RenameTableEvent) event)
                                                                    .getOldTableId();
                                                    Schema schema = schemas.get(oldTableId);
                                                    schemas.remove(oldTableId);
                                                    //
                                                    // schemas.put(((RenameTableEvent)
                                                    // event).getNewTableId(), schema);
                                                    schemas.put(
                                                            ((RenameTableEvent) event)
                                                                    .getNewTableId(),
                                                            ((RenameTableEvent) event).getSchema());
                                                    return event;
                                                }
                                                // other events are waiting to support
                                                return event;
                                            }
                                            return event.toString();
                                        }
                                    });

            sinkRow.print();

            env.execute("KafkaJsonEventSource Example");
        }
    }

    /** Runtime converter to convert {@link RecordData} type object to doris field. */
    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        Object serialize(int index, RecordData field);
    }

    static SerializationConverter createNullableExternalConverter(
            DataType type, ZoneId pipelineZoneId) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type, pipelineZoneId));
    }

    static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            } else {
                return serializationConverter.serialize(index, val);
            }
        };
    }

    // from doris pipeline, don't need to handle Heterogeneous data sources, the DataType is CDC
    // type.
    static SerializationConverter createExternalConverter(DataType type, ZoneId pipelineZoneId) {
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
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal();
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (index, val) ->
                        val.getTimestamp(index, DataTypeChecks.getPrecision(type))
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (index, val) ->
                        ZonedDateTime.ofInstant(
                                        val.getLocalZonedTimestampData(
                                                        index, DataTypeChecks.getPrecision(type))
                                                .toInstant(),
                                        pipelineZoneId)
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, zonedP).toTimestamp();
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) ->
                        writeValueAsString(convertRowData(val, index, type, pipelineZoneId));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private static List<Object> convertArrayData(ArrayData array, DataType type) {
        if (array instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) array).toObjectArray());
        }
        throw new UnsupportedOperationException("Unsupported array data: " + array.getClass());
    }

    private static Object convertMapData(MapData map, DataType type) {
        Map<Object, Object> result = new HashMap<>();
        if (map instanceof GenericMapData) {
            GenericMapData gMap = (GenericMapData) map;
            for (Object key : ((GenericArrayData) gMap.keyArray()).toObjectArray()) {
                result.put(key, gMap.get(key));
            }
            return result;
        }
        throw new UnsupportedOperationException("Unsupported map data: " + map.getClass());
    }

    private static Object convertRowData(
            RecordData val, int index, DataType type, ZoneId pipelineZoneId) {
        RowType rowType = (RowType) type;
        Map<String, Object> value = new HashMap<>();
        RecordData row = val.getRow(index, rowType.getFieldCount());

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField rowField = fields.get(i);
            SerializationConverter converter =
                    createNullableExternalConverter(rowField.getType(), pipelineZoneId);
            Object valTmp = converter.serialize(i, row);
            value.put(rowField.getName(), valTmp.toString());
        }
        return value;
    }

    private static String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
