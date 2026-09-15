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

package org.apache.flink.cdc.connectors.kafkajson.unit.pipeline;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisDdlBuilder;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonEventDeserializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.Configuration;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replays the history-record document the source produced for a <b>captured</b> canal message — the
 * very bytes that travel from the source connector to this pipeline — and follows it to the Doris
 * DDL the sink would execute.
 *
 * <p>The fixture {@code kafkajson/captured/mysql-canal-swap-rename-history.json} was dumped from
 * {@code KafkaJsonSchemaChangeHandler} while it handled {@code
 * kafkajson/captured/mysql-canal-swap/09-rename-three-pair-swap.json}, a message captured from
 * canal-server 1.1.8 on MySQL 8.0 (see the capture recipe in the source connector's {@code
 * captured/README.md}). The captured statement is the swap idiom — {@code RENAME TABLE a TO a_tmp,
 * b TO a, a_tmp TO b} — which is the only way MySQL lets two tables exchange names, and it is the
 * shape that has to survive the whole way to the sink: MySQL refuses the direct {@code RENAME TABLE
 * a TO b, b TO a} with {@code ERROR 1050}, so a real deployment can only ever produce the
 * three-pair form, and Doris can exchange two names in one atomic statement.
 */
public class KafkaJsonRenameFixtureTest {

    private static final TableId A = TableId.tableId("inventory", "a");
    private static final TableId B = TableId.tableId("inventory", "b");

    private final KafkaJsonEventDeserializer deserializer =
            new KafkaJsonEventDeserializer(DebeziumChangelogMode.ALL, true);

    @Test
    public void testCapturedSwapReachesDorisAsOneAtomicSwap() throws Exception {
        List<? extends Event> events = deserializer.deserialize(recordOfFixture());

        // the three pairs of the statement arrive as the two renames a sink can act on: the
        // temporary name is created and dropped inside the statement, so nothing carries it
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(RenameTableEvent.class);
        RenameTableEvent rename = (RenameTableEvent) events.get(0);
        assertThat(rename.getPairs()).hasSize(2);
        assertThat(rename.getPairs().get(0).getOldTableId()).isEqualTo(A);
        assertThat(rename.getPairs().get(0).getNewTableId()).isEqualTo(B);
        assertThat(rename.getPairs().get(1).getOldTableId()).isEqualTo(B);
        assertThat(rename.getPairs().get(1).getNewTableId()).isEqualTo(A);
        // each pair carries the schema of the table it moves, so the sink can convert rows of the
        // new id without waiting for a CREATE the source would never send
        assertThat(rename.getPairs().get(0).getSchema().getColumns()).hasSize(2);
        assertThat(rename.getPairs().get(1).getSchema().getColumns()).hasSize(2);
        // the statement itself is preserved for the audit trail
        assertThat(rename.getSql()).isEqualTo("RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b");

        // Doris exchanges the two names with one atomic statement, which no failed step can leave
        // half applied and which never exposes a window where neither table carries its final name
        List<String> sqls =
                new DorisDdlBuilder(new DorisDataSinkOptions(new Configuration()))
                        .buildRenameTableSql(rename);
        assertThat(sqls)
                .containsExactly(
                        "ALTER TABLE `inventory`.`a` REPLACE WITH TABLE `b` "
                                + "PROPERTIES('swap' = 'true')");
    }

    /**
     * Wraps the captured history document in the schema-change record the deserializer reads: the
     * key schema's name is what marks a record as a schema change, and the {@code source} struct
     * announces the table the producer named — for a canal rename that is the <b>post</b>-rename
     * name of the statement's first pair, {@code a_tmp} here.
     */
    private static SourceRecord recordOfFixture() throws IOException {
        Schema keySchema =
                SchemaBuilder.struct()
                        .name(KafkaJsonEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                        .build();
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("source", sourceSchema)
                        .field("historyRecord", Schema.STRING_SCHEMA)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(
                                "source",
                                new Struct(sourceSchema)
                                        .put("db", "inventory")
                                        .put("table", "a_tmp"))
                        .put("historyRecord", fixture());
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "inventory",
                keySchema,
                new Struct(keySchema),
                valueSchema,
                value);
    }

    private static String fixture() throws IOException {
        String resource = "kafkajson/captured/mysql-canal-swap-rename-history.json";
        try (InputStream in =
                KafkaJsonRenameFixtureTest.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("Missing test fixture " + resource);
            }
            try (Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name())) {
                return scanner.useDelimiter("\\A").next().trim();
            }
        }
    }
}
