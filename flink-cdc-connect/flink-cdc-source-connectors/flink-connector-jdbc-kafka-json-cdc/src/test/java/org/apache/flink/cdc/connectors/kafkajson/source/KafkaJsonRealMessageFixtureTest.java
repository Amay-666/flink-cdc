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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage.MessageType;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonParserFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.document.Array;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the raw messages captured from the four producer/format combinations — {@code tidb-canal},
 * {@code mysql-canal}, {@code tidb-debezium} and {@code mysql-debezium} — through the real parser,
 * schema-change handler and record converter, one sequence per directory (see {@code
 * src/test/resources/kafkajson/README.md}).
 *
 * <p>The sequences reproduce the reported failure: a canal message announces the <b>post</b>-rename
 * name for a rename and a Debezium schema-change record announces no table at all, so a connector
 * that reads the pre-rename name out of the message cannot find the schema to move and every data
 * message of the renamed table afterwards fails to decode ({@code IllegalStateException: No table
 * schema registered for test.test_schema_change3}). Both names therefore come from the DDL
 * statement, which these fixtures assert against real captures instead of hand-written messages.
 */
class KafkaJsonRealMessageFixtureTest {

    private static final String FIXTURE_ROOT = "kafkajson/";
    private static final long KAFKA_OFFSET = 7L;

    @Test
    void testTidbCanalRenameThenUpdateDecodes() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(MessageFormat.CANAL, DdlParser.DRUID);
        TableId oldId = new TableId("test", null, "test_schema_change4");
        TableId newId = new TableId("test", null, "test_schema_change3");

        // the captured CREATE registers the table, exactly as the JDBC snapshot would
        KafkaJsonMessage create = message(MessageFormat.CANAL, "tidb-canal/create-table.json");
        assertEquals(MessageType.DDL, create.getMessageType());
        assertEquals("test", create.getDatabase());
        assertEquals("test_schema_change4", create.getTable());
        handle(context, create);
        Table created = context.getDatabaseSchema().tableFor(oldId);
        assertEquals(6, created.columns().size());
        assertEquals("called_phone", created.columnWithName("called_phone").name());
        // the CREATE is a schema change of its own and enqueues a record; drain it here so that the
        // record asserted on below is the rename's
        assertEquals(1, drain(context.getQueue(), 1).size());

        // the captured RENAME announces the *new* name, so the announced table is not the one the
        // schema is registered under -- the trap this fixture exists for
        KafkaJsonMessage rename = message(MessageFormat.CANAL, "tidb-canal/rename-table.json");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("test_schema_change3", rename.getTable());
        assertEquals(
                "RENAME TABLE `test_schema_change4` TO `test_schema_change3`", rename.getSql());
        assertNull(context.getDatabaseSchema().tableFor(newId));

        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(oldId));
        Table renamed = context.getDatabaseSchema().tableFor(newId);
        assertNotNull(renamed);
        assertEquals(newId, renamed.id());
        assertEquals(6, renamed.columns().size());

        // both sides of the rename are reported from the statement, not from the message
        HistoryRecord historyRecord =
                SourceRecordUtils.getHistoryRecord(drain(context.getQueue(), 1).get(0));
        assertEquals(
                KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE,
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(1, pairs.size());
        assertEquals(
                "test.test_schema_change4",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.test_schema_change3",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));

        // the captured UPDATE that follows the rename decodes against the renamed schema; while the
        // rename was applied under the wrong name this threw "No table schema registered for
        // test.test_schema_change3"
        KafkaJsonMessage update =
                message(MessageFormat.CANAL, "tidb-canal/update-after-rename.json");
        assertEquals(MessageType.DML, update.getMessageType());
        List<SourceRecord> records =
                context.getRecordConverter().convert(update, "kafka-json-topic", 0, KAFKA_OFFSET);
        assertEquals(1, records.size());
        Struct value = (Struct) records.get(0).value();
        assertEquals("u", value.getString("op"));
        assertEquals("1123499817", String.valueOf(value.getStruct("after").get("called_phone")));
        assertEquals("1235.45", String.valueOf(value.getStruct("after").get("price")));
        // the before image carries the pre-UPDATE value and is completed from the unchanged columns
        assertEquals("1123499816", String.valueOf(value.getStruct("before").get("called_phone")));
        // the record is keyed by the announced primary key, which the DDL-derived schema provides
        assertEquals("1", String.valueOf(((Struct) records.get(0).key()).get("id")));
    }

    @Test
    void testMysqlCanalRenameAnnouncesTheOldName() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(MessageFormat.CANAL, DdlParser.DRUID);
        TableId oldId = new TableId("inventory", null, "orders");
        TableId newId = new TableId("inventory", null, "orders_archive");

        handle(context, message(MessageFormat.CANAL, "mysql-canal/create-table.json"));
        assertEquals(4, context.getDatabaseSchema().tableFor(oldId).columns().size());
        assertEquals(1, drain(context.getQueue(), 1).size());

        // the captured INSERT decodes before the rename
        KafkaJsonMessage insert = message(MessageFormat.CANAL, "mysql-canal/insert.json");
        List<SourceRecord> inserts =
                context.getRecordConverter().convert(insert, "kafka-json-topic", 0, KAFKA_OFFSET);
        assertEquals(1, inserts.size());
        assertEquals("c", ((Struct) inserts.get(0).value()).getString("op"));

        // canal on MySQL fills the same field with the *old* name that TiCDC fills with the new
        // one,
        // which is why neither producer's `table` can be trusted for a rename
        KafkaJsonMessage rename = message(MessageFormat.CANAL, "mysql-canal/rename-table.json");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("orders", rename.getTable());
        assertEquals("RENAME TABLE `orders` TO `orders_archive`", rename.getSql());

        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(oldId));
        assertEquals(4, context.getDatabaseSchema().tableFor(newId).columns().size());
        HistoryRecord historyRecord =
                SourceRecordUtils.getHistoryRecord(drain(context.getQueue(), 1).get(0));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(1, pairs.size());
        assertEquals(
                "inventory.orders",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.orders_archive",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
    }

    @Test
    void testTidbDebeziumRenameCarriesNoTableAndStillApplies() throws Exception {
        KafkaJsonSourceFetchTaskContext context =
                context(MessageFormat.DEBEZIUM, DdlParser.DEBEZIUM);
        TableId oldId = new TableId("test", null, "test_schema_change4");
        TableId newId = new TableId("test", null, "test_schema_change3");

        // The snapshot phase registers the table over JDBC; the CREATE that TiCDC captured for this
        // same table stands in for it here, since a DDL message is read for its database, table and
        // SQL only — never for the format it arrived in.
        handle(context, message(MessageFormat.CANAL, "tidb-canal/create-table.json"));
        assertEquals(6, context.getDatabaseSchema().tableFor(oldId).columns().size());
        // the CREATE's own schema-change record is drained so that the record read back after the
        // rename is the rename's
        assertEquals(1, drain(context.getQueue(), 1).size());

        // TiCDC's debezium protocol announces the database and the DDL of a schema change but no
        // table at all, so the pre-rename name has to come from the statement
        KafkaJsonMessage rename =
                message(MessageFormat.DEBEZIUM, "tidb-debezium/rename-table-ddl.json");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("test", rename.getDatabase());
        assertNull(rename.getTable());
        assertEquals(
                "RENAME TABLE `test_schema_change4` TO `test_schema_change3`", rename.getSql());

        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(oldId));
        assertEquals(6, context.getDatabaseSchema().tableFor(newId).columns().size());
        HistoryRecord historyRecord =
                SourceRecordUtils.getHistoryRecord(drain(context.getQueue(), 1).get(0));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(1, pairs.size());
        assertEquals(
                "test.test_schema_change4",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.test_schema_change3",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));

        // the DML that follows the rename decodes against the renamed schema
        KafkaJsonMessage update =
                message(MessageFormat.DEBEZIUM, "tidb-debezium/update-after-rename.json");
        List<SourceRecord> records =
                context.getRecordConverter().convert(update, "kafka-json-topic", 0, KAFKA_OFFSET);
        assertEquals(1, records.size());
        Struct value = (Struct) records.get(0).value();
        assertEquals("u", value.getString("op"));
        assertEquals("1123499817", String.valueOf(value.getStruct("after").get("called_phone")));
        assertEquals("1123499816", String.valueOf(value.getStruct("before").get("called_phone")));
        assertEquals("1", String.valueOf(((Struct) records.get(0).key()).get("id")));
    }

    @Test
    void testMysqlDebeziumRenameCarriesNoTableAndStillApplies() throws Exception {
        KafkaJsonSourceFetchTaskContext context =
                context(MessageFormat.DEBEZIUM, DdlParser.DEBEZIUM);
        TableId oldId = new TableId("inventory", null, "orders");
        TableId newId = new TableId("inventory", null, "orders_archive");

        // the snapshot phase observed the table before the rename
        handle(context, message(MessageFormat.CANAL, "mysql-canal/create-table.json"));
        assertEquals(4, context.getDatabaseSchema().tableFor(oldId).columns().size());
        assertEquals(1, drain(context.getQueue(), 1).size());

        // a Debezium message has no mysqlType to fall back on: the columns and their types are read
        // from the registered schema alone, so the pre-rename INSERT only decodes while the table
        // is
        // still registered under the name the message announces
        KafkaJsonMessage insertBefore =
                message(MessageFormat.DEBEZIUM, "mysql-debezium/insert.json");
        assertEquals("orders", insertBefore.getTable());
        List<SourceRecord> preRename =
                context.getRecordConverter()
                        .convert(insertBefore, "kafka-json-topic", 0, KAFKA_OFFSET);
        assertEquals(1, preRename.size());
        assertEquals("c", ((Struct) preRename.get(0).value()).getString("op"));

        // the Debezium MySQL connector writes a DDL to the schema-history topic as a bare history
        // record: the database and the DDL, no table
        KafkaJsonMessage rename =
                message(MessageFormat.DEBEZIUM, "mysql-debezium/rename-table-ddl.json");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("inventory", rename.getDatabase());
        assertNull(rename.getTable());
        assertEquals("RENAME TABLE `orders` TO `orders_archive`", rename.getSql());

        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(oldId));
        Table renamed = context.getDatabaseSchema().tableFor(newId);
        assertNotNull(renamed);
        assertEquals(4, renamed.columns().size());
        assertTrue(renamed.columns().stream().anyMatch(column -> "amount".equals(column.name())));
        HistoryRecord historyRecord =
                SourceRecordUtils.getHistoryRecord(drain(context.getQueue(), 1).get(0));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(1, pairs.size());
        assertEquals(
                "inventory.orders",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.orders_archive",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));

        // the pre-rename INSERT is no longer decodable, exactly as on the source: the rename moved
        // the schema to the new name and nothing is registered under the old one any more
        assertEquals(
                0,
                context.getRecordConverter()
                        .convert(insertBefore, "kafka-json-topic", 0, KAFKA_OFFSET)
                        .size());
    }

    /** Reads a fixture from the {@code kafkajson/} test resources. */
    private static String fixture(String name) throws Exception {
        URI location =
                KafkaJsonRealMessageFixtureTest.class
                        .getClassLoader()
                        .getResource(FIXTURE_ROOT + name)
                        .toURI();
        return new String(Files.readAllBytes(Paths.get(location)), StandardCharsets.UTF_8);
    }

    /** Parses a fixture with the parser of its own wire format. */
    private static KafkaJsonMessage message(MessageFormat format, String fixture) throws Exception {
        KafkaJsonMessage message = KafkaJsonParserFactory.create(format).parse(fixture(fixture));
        assertNotNull(message, fixture);
        return message;
    }

    private static void handle(KafkaJsonSourceFetchTaskContext context, KafkaJsonMessage message)
            throws Exception {
        new KafkaJsonSchemaChangeHandler(context.getSourceConfig())
                .handle(context, message, offsetOf(message));
    }

    private static KafkaJsonOffset offsetOf(KafkaJsonMessage message) {
        Long eventTime = message.getEventTimeValue(EventTime.ES);
        return new KafkaJsonOffset(eventTime == null ? 0L : eventTime, 0, KAFKA_OFFSET);
    }

    private static KafkaJsonSourceFetchTaskContext context(
            MessageFormat format, DdlParser ddlParser) {
        KafkaJsonSourceConfig config =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test", "inventory")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .messageFormat(format)
                        .ddlParser(ddlParser)
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(true)
                        .create(0);
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        KafkaJsonOffset.INITIAL_OFFSET,
                        KafkaJsonOffset.NO_STOPPING_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        context.configure(split);
        return context;
    }

    /** Polls the queue until {@code expected} records have been drained (or a timeout elapses). */
    private static List<SourceRecord> drain(ChangeEventQueue<DataChangeEvent> queue, int expected)
            throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline && records.size() < expected) {
            for (DataChangeEvent event : queue.poll()) {
                records.add(event.getRecord());
            }
        }
        return records;
    }
}
