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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisRowConverter;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link DorisRowConverter}. */
public class DorisRowConverterTest {

    private static final ZoneId PIPELINE_ZONE = ZoneId.of("Asia/Shanghai");

    @Test
    public void testConvertScalars() {
        Schema schema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(32)))
                        .column(Column.physicalColumn("active", DataTypes.BOOLEAN()))
                        .column(Column.physicalColumn("score", DataTypes.DOUBLE()))
                        .column(Column.physicalColumn("amount", DataTypes.DECIMAL(10, 2)))
                        .column(Column.physicalColumn("day", DataTypes.DATE()))
                        .column(Column.physicalColumn("clock", DataTypes.TIME()))
                        .column(Column.physicalColumn("ts", DataTypes.TIMESTAMP(6)))
                        .column(Column.physicalColumn("ts_ltz", DataTypes.TIMESTAMP_LTZ(6)))
                        .column(Column.physicalColumn("ts_tz", DataTypes.TIMESTAMP_TZ(6)))
                        .build();

        BinaryRecordData record =
                new BinaryRecordDataGenerator(
                                RowType.of(
                                        DataTypes.INT(),
                                        DataTypes.VARCHAR(32),
                                        DataTypes.BOOLEAN(),
                                        DataTypes.DOUBLE(),
                                        DataTypes.DECIMAL(10, 2),
                                        DataTypes.DATE(),
                                        DataTypes.TIME(),
                                        DataTypes.TIMESTAMP(6),
                                        DataTypes.TIMESTAMP_LTZ(6),
                                        DataTypes.TIMESTAMP_TZ(6)))
                        .generate(
                                new Object[] {
                                    7,
                                    BinaryStringData.fromString("doris"),
                                    true,
                                    1.5d,
                                    DecimalData.fromBigDecimal(
                                            new BigDecimal("12.34"), 10, 2),
                                    (int) LocalDate.of(2024, 5, 6).toEpochDay(),
                                    (int) (LocalTime.of(10, 30, 45).toNanoOfDay() / 1_000_000),
                                    TimestampData.fromLocalDateTime(
                                            LocalDateTime.of(
                                                    2024, 5, 6, 10, 30, 45, 123_456_000)),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2024-05-06T02:30:45.123456Z")),
                                    ZonedTimestampData.fromOffsetDateTime(
                                            OffsetDateTime.parse(
                                                    "2024-05-06T10:30:45.123456+08:00"))
                                });

        Map<String, Object> row = new DorisRowConverter(schema, PIPELINE_ZONE).convert(record, schema);

        assertThat(row.get("id")).isEqualTo(7);
        assertThat(row.get("name")).isEqualTo("doris");
        assertThat(row.get("active")).isEqualTo(true);
        assertThat(row.get("score")).isEqualTo(1.5d);
        assertThat(row.get("amount")).isEqualTo(new BigDecimal("12.34"));
        assertThat(row.get("day")).isEqualTo("2024-05-06");
        assertThat(row.get("clock")).isEqualTo("10:30:45");
        assertThat(row.get("ts")).isEqualTo("2024-05-06 10:30:45.123456");
        // An instant at 02:30:45Z renders as 10:30:45 in Asia/Shanghai (UTC+8).
        assertThat(row.get("ts_ltz")).isEqualTo("2024-05-06 10:30:45.123456");
        // A zoned timestamp is shifted to the pipeline zone before rendering.
        assertThat(row.get("ts_tz")).isEqualTo("2024-05-06 10:30:45.123456");
    }

    @Test
    public void testConvertComplexTypes() {
        Schema schema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("tags", DataTypes.ARRAY(DataTypes.VARCHAR(16))))
                        .column(
                                Column.physicalColumn(
                                        "attrs", DataTypes.MAP(DataTypes.VARCHAR(16), DataTypes.INT())))
                        .column(
                                Column.physicalColumn(
                                        "point",
                                        DataTypes.ROW(
                                                new org.apache.flink.cdc.common.types.DataField(
                                                        "x", DataTypes.INT()),
                                                new org.apache.flink.cdc.common.types.DataField(
                                                        "label", DataTypes.VARCHAR(16)))))
                        .column(Column.physicalColumn("nullable", DataTypes.INT()))
                        .build();

        // BinaryRecordDataGenerator cannot build MAP fields, so the record is hand-rolled as a
        // FakeRecordData holding already-materialized nested values.
        RecordData point = new FakeRecordData(1, BinaryStringData.fromString("p"));
        Map<Object, Object> attrs = new LinkedHashMap<>();
        attrs.put(BinaryStringData.fromString("k"), 1);

        RecordData record =
                new FakeRecordData(
                        new GenericArrayData(
                                new Object[] {
                                    BinaryStringData.fromString("a"),
                                    BinaryStringData.fromString("b")
                                }),
                        new GenericMapData(attrs),
                        point,
                        null);

        Map<String, Object> row = new DorisRowConverter(schema, PIPELINE_ZONE).convert(record, schema);

        // ARRAY/MAP/ROW render as JSON strings, matching the STRING column type chosen by
        // DorisDdlBuilder.
        assertThat(row.get("tags")).isEqualTo("[\"a\",\"b\"]");
        assertThat(row.get("attrs")).isEqualTo("{\"k\":1}");
        assertThat(row.get("point")).isEqualTo("{\"x\":1,\"label\":\"p\"}");
        assertThat(row.get("nullable")).isNull();
    }

    @Test
    public void testNullFieldsRenderAsNull() {
        Schema schema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(16)))
                        .build();
        BinaryRecordData record =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.VARCHAR(16)))
                        .generate(new Object[] {null, null});

        Map<String, Object> row = new DorisRowConverter(schema, PIPELINE_ZONE).convert(record, schema);

        assertThat(row.get("id")).isNull();
        assertThat(row.get("name")).isNull();
    }

    @Test
    public void testSchemaEvolutionRebuildsConverters() {
        Schema v1 =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(16)))
                        .build();
        Schema v2 =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(16)))
                        .column(Column.physicalColumn("extra", DataTypes.BIGINT()))
                        .build();

        DorisRowConverter converter = new DorisRowConverter(v1, PIPELINE_ZONE);
        BinaryRecordData recordV1 =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.VARCHAR(16)))
                        .generate(new Object[] {1, BinaryStringData.fromString("a")});
        assertThat(converter.convert(recordV1, v1)).containsOnlyKeys("id", "name");

        // The same converter instance follows the schema to its next version.
        BinaryRecordData recordV2 =
                new BinaryRecordDataGenerator(
                                RowType.of(DataTypes.INT(), DataTypes.VARCHAR(16), DataTypes.BIGINT()))
                        .generate(new Object[] {2, BinaryStringData.fromString("b"), 9L});
        assertThat(converter.convert(recordV2, v2))
                .containsExactlyInAnyOrderEntriesOf(
                        new LinkedHashMap<String, Object>() {
                            {
                                put("id", 2);
                                put("name", "b");
                                put("extra", 9L);
                            }
                        });
    }

    /**
     * A {@link RecordData} backed by pre-materialized values. {@code BinaryRecordDataGenerator} cannot
     * build MAP fields, so tests for the complex types hand-roll the record with this.
     */
    private static class FakeRecordData implements RecordData {
        private final Object[] values;

        FakeRecordData(Object... values) {
            this.values = values;
        }

        @Override
        public int getArity() {
            return values.length;
        }

        @Override
        public boolean isNullAt(int pos) {
            return values[pos] == null;
        }

        @Override
        public boolean getBoolean(int pos) {
            return (boolean) values[pos];
        }

        @Override
        public byte getByte(int pos) {
            return (byte) values[pos];
        }

        @Override
        public short getShort(int pos) {
            return (short) values[pos];
        }

        @Override
        public int getInt(int pos) {
            return (int) values[pos];
        }

        @Override
        public long getLong(int pos) {
            return (long) values[pos];
        }

        @Override
        public float getFloat(int pos) {
            return (float) values[pos];
        }

        @Override
        public double getDouble(int pos) {
            return (double) values[pos];
        }

        @Override
        public byte[] getBinary(int pos) {
            return (byte[]) values[pos];
        }

        @Override
        public StringData getString(int pos) {
            return (StringData) values[pos];
        }

        @Override
        public DecimalData getDecimal(int pos, int precision, int scale) {
            return (DecimalData) values[pos];
        }

        @Override
        public TimestampData getTimestamp(int pos, int precision) {
            return (TimestampData) values[pos];
        }

        @Override
        public ZonedTimestampData getZonedTimestamp(int pos, int precision) {
            return (ZonedTimestampData) values[pos];
        }

        @Override
        public LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision) {
            return (LocalZonedTimestampData) values[pos];
        }

        @Override
        public ArrayData getArray(int pos) {
            return (ArrayData) values[pos];
        }

        @Override
        public MapData getMap(int pos) {
            return (MapData) values[pos];
        }

        @Override
        public RecordData getRow(int pos, int numFields) {
            return (RecordData) values[pos];
        }
    }
}
