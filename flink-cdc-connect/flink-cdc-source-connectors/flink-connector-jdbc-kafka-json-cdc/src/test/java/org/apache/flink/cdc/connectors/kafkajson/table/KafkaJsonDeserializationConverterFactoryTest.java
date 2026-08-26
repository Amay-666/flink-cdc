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

package org.apache.flink.cdc.connectors.kafkajson.table;

import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.data.EnumSet;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonDeserializationConverterFactory}. */
class KafkaJsonDeserializationConverterFactoryTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    private final DeserializationRuntimeConverterFactory factory =
            KafkaJsonDeserializationConverterFactory.instance();

    @Test
    void testTinyIntConverter() throws Exception {
        DeserializationRuntimeConverter converter =
                factory.createUserDefinedConverter(new TinyIntType(), UTC).get();

        assertEquals((byte) 1, converter.convert(Boolean.TRUE, Schema.INT8_SCHEMA));
        assertEquals((byte) 0, converter.convert(Boolean.FALSE, Schema.INT8_SCHEMA));
        assertEquals((byte) 5, converter.convert((byte) 5, Schema.INT8_SCHEMA));
        assertEquals((byte) 5, converter.convert("5", Schema.STRING_SCHEMA));
    }

    @Test
    void testSetArrayConverter() throws Exception {
        // Flink ARRAY<STRING> column <- MySQL SET value rendered as "a,b,c"
        LogicalType arrayType = new ArrayType(new VarCharType());
        DeserializationRuntimeConverter converter =
                factory.createUserDefinedConverter(arrayType, UTC).get();

        Schema enumSetSchema =
                SchemaBuilder.struct()
                        .name(EnumSet.LOGICAL_NAME)
                        .field("data", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        GenericArrayData result = (GenericArrayData) converter.convert("a,b,c", enumSetSchema);

        assertEquals(3, result.size());
        assertEquals(StringData.fromString("a"), result.getString(0));
        assertEquals(StringData.fromString("b"), result.getString(1));
        assertEquals(StringData.fromString("c"), result.getString(2));
    }

    @Test
    void testDefaultFallback() {
        // no user-defined converter for numeric types -> the default converter is used
        Optional<DeserializationRuntimeConverter> converter =
                factory.createUserDefinedConverter(new BigIntType(), UTC);
        assertFalse(converter.isPresent());
    }

    @Test
    void testUserDefinedConvertersAreRegistered() {
        assertTrue(factory.createUserDefinedConverter(new TinyIntType(), UTC).isPresent());
        assertTrue(factory.createUserDefinedConverter(new VarCharType(), UTC).isPresent());
        assertTrue(
                factory.createUserDefinedConverter(new ArrayType(new VarCharType()), UTC)
                        .isPresent());
    }
}
