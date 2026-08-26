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

import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for the metadata converters of {@link KafkaJsonReadableMetadata}. */
class KafkaJsonReadableMetadataTest {

    @Test
    void testTableNameAndDatabaseName() {
        SourceRecord record = createRecord();

        MetadataConverter tableNameConverter = KafkaJsonReadableMetadata.TABLE_NAME.getConverter();
        MetadataConverter databaseNameConverter =
                KafkaJsonReadableMetadata.DATABASE_NAME.getConverter();

        assertEquals(StringData.fromString("users"), tableNameConverter.read(record));
        assertEquals(StringData.fromString("test"), databaseNameConverter.read(record));
    }

    @Test
    void testTimestamps() {
        SourceRecord record = createRecord();

        assertEquals(
                TimestampData.fromEpochMillis(1000L),
                KafkaJsonReadableMetadata.OP_TS.getConverter().read(record));
        assertEquals(
                TimestampData.fromEpochMillis(900L),
                KafkaJsonReadableMetadata.ES.getConverter().read(record));
        assertEquals(
                TimestampData.fromEpochMillis(1100L),
                KafkaJsonReadableMetadata.TS.getConverter().read(record));
    }

    @Test
    void testRowKindIsNotReadableFromRecord() {
        SourceRecord record = createRecord();

        // ROW_KIND is derived from the RowData row kind, so reading it from a SourceRecord is
        // rejected
        assertThrows(
                UnsupportedOperationException.class,
                () -> KafkaJsonReadableMetadata.ROW_KIND.getConverter().read(record));
    }

    private static SourceRecord createRecord() {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("ts_ms", Schema.INT64_SCHEMA)
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("es", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("ts", Schema.OPTIONAL_INT64_SCHEMA)
                        .build();
        Struct source =
                new Struct(sourceSchema)
                        .put("ts_ms", 1000L)
                        .put("db", "test")
                        .put("table", "users")
                        .put("es", 900L)
                        .put("ts", 1100L);

        Schema valueSchema =
                SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, sourceSchema).build();
        Struct value = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, source);

        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "canal_data", valueSchema, value);
    }
}
