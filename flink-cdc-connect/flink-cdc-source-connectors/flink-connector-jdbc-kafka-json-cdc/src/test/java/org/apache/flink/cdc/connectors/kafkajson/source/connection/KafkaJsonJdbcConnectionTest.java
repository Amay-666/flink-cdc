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

package org.apache.flink.cdc.connectors.kafkajson.source.connection;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonJdbcConnection}.
 *
 * <p>The base {@link io.debezium.jdbc.JdbcConnection#readTableColumn} reads the column default
 * ({@code COLUMN_DEF}, {@code getString(13)}) and hands it to Debezium's {@code TableSchemaBuilder} as
 * the {@code defaultValueExpression}. For binary-typed columns the MySQL driver reports that default as
 * a literal string (e.g. {@code 0x}), which {@code SchemaBuilder.defaultValue} refuses to convert to a
 * {@code BYTES} default and the snapshot schema read aborts with {@code DebeziumException: Failed to
 * set field default value ...}. The override must therefore read the column metadata without ever
 * touching column 13.
 */
class KafkaJsonJdbcConnectionTest {

    @Test
    void readTableColumnSkipsTheColumnDefault() throws Exception {
        // COLUMN_DEF is column 13 of DatabaseMetaData#getColumns; the fake ResultSet records every
        // column index read through getString so the test can prove the default is never fetched.
        Set<Integer> accessed = new HashSet<>();
        ResultSet columnMetadata =
                (ResultSet)
                        Proxy.newProxyInstance(
                                ResultSet.class.getClassLoader(),
                                new Class<?>[] {ResultSet.class},
                                new RecordingMetadataHandler(accessed));

        KafkaJsonJdbcConnection connection =
                new KafkaJsonJdbcConnection(
                        JdbcConfiguration.create()
                                .with("database.hostname", "localhost")
                                .build(),
                        (ConnectionFactory) config -> null,
                        "`",
                        "`");

        Optional<ColumnEditor> editor =
                connection.readTableColumn(
                        columnMetadata, new TableId("prober", null, "defs2"), null);

        // the default value is never read...
        assertThat(accessed).doesNotContain(13);
        // ...so the editor carries no defaultValueExpression
        assertThat(editor).isPresent();
        assertThat(editor.get().defaultValueExpression()).isEmpty();
        // while the rest of the metadata is preserved
        Column column = editor.get().create();
        assertThat(column.name()).isEqualTo("m");
        assertThat(column.typeName()).isEqualTo("BINARY");
        assertThat(column.length()).isEqualTo(8);
        assertThat(column.position()).isEqualTo(3);
        assertThat(column.jdbcType()).isEqualTo(Types.BINARY);
        assertThat(column.isOptional()).isTrue();
        assertThat(column.isAutoIncremented()).isFalse();
        assertThat(column.isGenerated()).isFalse();
    }

    /** Returns fake {@link DatabaseMetaData#getColumns} values; records every getString index. */
    private static class RecordingMetadataHandler implements InvocationHandler {

        private final Set<Integer> accessed;

        RecordingMetadataHandler(Set<Integer> accessed) {
            this.accessed = accessed;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "getString":
                    int index = (Integer) args[0];
                    accessed.add(index);
                    switch (index) {
                        case 4:
                            return "m";
                        case 6:
                            return "BINARY";
                        case 13:
                            return "0x";
                        case 23:
                        case 24:
                            return "NO";
                        default:
                            return null;
                    }
                case "getInt":
                    switch ((Integer) args[0]) {
                        case 5:
                            return Types.BINARY;
                        case 7:
                            return 8;
                        case 11:
                            return DatabaseMetaData.columnNullable;
                        case 17:
                            return 3;
                        default:
                            return 0;
                    }
                case "getObject":
                    return null;
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "toString":
                    return "RecordingMetadataHandler";
                default:
                    return null;
            }
        }
    }
}
