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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replays the messages captured from four real producer/connector combinations, byte for byte as
 * they arrived on the topic, through the real parser, schema-change handler and record converter.
 *
 * <p>The fixtures live in {@code src/test/resources/kafkajson/captured/} and were produced by the
 * capture scripts documented in {@code captured/README.md}; each test replays one session in the
 * order the broker delivered it. Unlike a hand-written fixture, these carry the producers' own
 * quirks, which is what the session tests assert on: the table name a rename announces, whether a
 * multi-pair {@code RENAME TABLE} arrives as one message or one per pair, and which producers send
 * DDL at all.
 */
class KafkaJsonCapturedMessageTest {

    private static final String ROOT = "kafkajson/captured/";
    private static final long KAFKA_OFFSET = 7L;

    /**
     * canal-server 1.1.8 on MySQL 8.0, one topic partition, one session: create, insert, update,
     * rename, DML on the renamed table, a two-pair rename, a rename written as {@code ALTER},
     * truncate, drop and an added column.
     */
    @Test
    void testCanalServerSession() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(MessageFormat.CANAL, DdlParser.DRUID);
        TableId orders = new TableId("inventory", null, "orders");
        TableId archive = new TableId("inventory", null, "orders_archive");

        // the session opens by dropping and recreating the database; neither names a table
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "01-session-reset-drop-database");
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "02-session-reset-create-database");
        assertTrue(drainAll(context.getQueue()).isEmpty());

        handleStep(context, MessageFormat.CANAL, "mysql-canal", "03-create-table-orders");
        assertEquals(4, context.getDatabaseSchema().tableFor(orders).columns().size());
        drainAll(context.getQueue());

        // canal echoes the original statement of every row event as a `QUERY` message with no rows
        // (binlog_rows_query_log_events=ON); it must contribute nothing
        assertEquals(
                0,
                convert(context, MessageFormat.CANAL, "mysql-canal", "04-rows-query-insert-orders")
                        .size());

        List<SourceRecord> inserted =
                convert(context, MessageFormat.CANAL, "mysql-canal", "05-insert-orders");
        assertEquals(1, inserted.size());
        assertEquals("c", ((Struct) inserted.get(0).value()).getString("op"));
        assertEquals(
                "1001",
                String.valueOf(
                        ((Struct) inserted.get(0).value()).getStruct("after").get("customer_id")));

        assertEquals(
                0,
                convert(context, MessageFormat.CANAL, "mysql-canal", "06-rows-query-update-orders")
                        .size());
        Struct updated =
                (Struct)
                        convert(context, MessageFormat.CANAL, "mysql-canal", "07-update-orders")
                                .get(0)
                                .value();
        assertEquals("u", updated.getString("op"));
        assertEquals("20.0", String.valueOf(updated.getStruct("after").get("amount")));
        // canal's `old` array carries only the changed columns; the rest is completed from `after`
        assertEquals("1001", String.valueOf(updated.getStruct("before").get("customer_id")));
        assertEquals("12.34", String.valueOf(updated.getStruct("before").get("amount")));

        // the rename announces the *new* name in `table` -- the trap that made the old lookup miss
        KafkaJsonMessage rename = message(MessageFormat.CANAL, "mysql-canal", "08-rename-table");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("orders_archive", rename.getTable());
        assertEquals("RENAME TABLE orders TO orders_archive", rename.getSql());
        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(orders));
        assertEquals(4, context.getDatabaseSchema().tableFor(archive).columns().size());
        Array pairs = renamePairs(context);
        assertEquals(1, pairs.size());
        assertEquals(
                "inventory.orders",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.orders_archive",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));

        // data of the renamed table now decodes: the whole point of the fix
        Struct afterRenameInsert =
                (Struct)
                        convert(
                                        context,
                                        MessageFormat.CANAL,
                                        "mysql-canal",
                                        "10-insert-orders_archive")
                                .get(0)
                                .value();
        assertEquals("c", afterRenameInsert.getString("op"));
        assertEquals("55.55", String.valueOf(afterRenameInsert.getStruct("after").get("amount")));
        assertEquals(
                "d",
                ((Struct)
                                convert(
                                                context,
                                                MessageFormat.CANAL,
                                                "mysql-canal",
                                                "14-delete-orders_archive")
                                        .get(0)
                                        .value())
                        .getString("op"));

        handleStep(context, MessageFormat.CANAL, "mysql-canal", "15-create-table-t1");
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "16-create-table-t2");
        drainAll(context.getQueue());

        // one statement renaming two tables reaches the connector as ONE message carrying both
        // pairs, which is what lets a sink migrate them together and see a name cycle
        KafkaJsonMessage multiPair =
                message(MessageFormat.CANAL, "mysql-canal", "21-rename-multi-pair");
        assertEquals("RENAME TABLE t1 TO t1_new, t2 TO t2_new", multiPair.getSql());
        assertEquals("t1_new", multiPair.getTable());
        handle(context, multiPair);
        assertNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1")));
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1_new")));
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t2_new")));
        Array multiPairs = renamePairs(context);
        assertEquals(2, multiPairs.size());
        assertEquals(
                "inventory.t1",
                multiPairs
                        .get(0)
                        .asDocument()
                        .getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.t2",
                multiPairs
                        .get(1)
                        .asDocument()
                        .getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));

        handleStep(context, MessageFormat.CANAL, "mysql-canal", "22-create-table-a");
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "23-create-table-b");
        drainAll(context.getQueue());

        // `ALTER TABLE ... RENAME TO` is reported as a RENAME too (never as an ALTER)
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "24-rename-table-via-alter");
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1_renamed")));
        drainAll(context.getQueue());

        // TRUNCATE keeps the table and its schema, DROP (canal calls it ERASE) removes it
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "25-truncate-table");
        assertEquals(
                KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_TRUNCATE_TABLE,
                historyRecord(context)
                        .document()
                        .getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1_renamed")));
        handleStep(context, MessageFormat.CANAL, "mysql-canal", "26-drop-table");
        assertNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t2_new")));

        handleStep(context, MessageFormat.CANAL, "mysql-canal", "27-alter-add-column");
        assertEquals(5, context.getDatabaseSchema().tableFor(archive).columns().size());
        assertNotNull(context.getDatabaseSchema().tableFor(archive).columnWithName("note"));
    }

    /**
     * TiCDC 8.5.1 canal-json on TiDB 8.5.1, one topic partition: the same shape of session through
     * the same format, but TiCDC splits a multi-pair {@code RENAME TABLE} into one message per pair
     * and writes the statement names with backticks and a database qualifier.
     */
    @Test
    void testTidbCanalSession() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(MessageFormat.CANAL, DdlParser.DRUID);
        TableId before = new TableId("test", null, "test_schema_change4");
        TableId after = new TableId("test", null, "test_schema_change3");

        handleStep(context, MessageFormat.CANAL, "tidb-canal", "01-session-reset-drop-database");
        handleStep(context, MessageFormat.CANAL, "tidb-canal", "02-session-reset-create-database");
        handleStep(
                context, MessageFormat.CANAL, "tidb-canal", "03-create-table-test_schema_change4");
        assertEquals(6, context.getDatabaseSchema().tableFor(before).columns().size());
        drainAll(context.getQueue());

        assertEquals(
                1,
                convert(context, MessageFormat.CANAL, "tidb-canal", "04-insert-test_schema_change4")
                        .size());
        assertEquals(
                1,
                convert(context, MessageFormat.CANAL, "tidb-canal", "05-update-test_schema_change4")
                        .size());

        KafkaJsonMessage rename = message(MessageFormat.CANAL, "tidb-canal", "06-rename-table");
        assertEquals("test_schema_change3", rename.getTable());
        assertEquals(
                "RENAME TABLE `test`.`test_schema_change4` TO `test`.`test_schema_change3`",
                rename.getSql());
        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(before));
        assertEquals(6, context.getDatabaseSchema().tableFor(after).columns().size());
        assertEquals(1, renamePairs(context).size());

        // the reported failure reproduced end to end: the UPDATE written after the rename decodes
        Struct update =
                (Struct)
                        convert(
                                        context,
                                        MessageFormat.CANAL,
                                        "tidb-canal",
                                        "07-update-test_schema_change3")
                                .get(0)
                                .value();
        assertEquals("u", update.getString("op"));
        assertEquals("1123499818", String.valueOf(update.getStruct("after").get("called_phone")));
        assertEquals("1123499817", String.valueOf(update.getStruct("before").get("called_phone")));
        assertEquals(
                1,
                convert(context, MessageFormat.CANAL, "tidb-canal", "08-insert-test_schema_change3")
                        .size());
        assertEquals(
                1,
                convert(context, MessageFormat.CANAL, "tidb-canal", "09-delete-test_schema_change3")
                        .size());

        handleStep(context, MessageFormat.CANAL, "tidb-canal", "10-create-table-r1");
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.CANAL, "tidb-canal", "11-create-table-r2");
        drainAll(context.getQueue());

        // `RENAME TABLE r1 TO r1_new, r2 TO r2_new` reaches the connector as two separate
        // single-pair messages, so this producer never delivers the pairs of one statement together
        KafkaJsonMessage split1 = message(MessageFormat.CANAL, "tidb-canal", "14-rename-table-r1");
        assertEquals("RENAME TABLE `test`.`r1` TO `test`.`r1_new`", split1.getSql());
        handle(context, split1);
        assertNotNull(context.getDatabaseSchema().tableFor(new TableId("test", null, "r1_new")));
        assertEquals(1, renamePairs(context).size());
        KafkaJsonMessage split2 = message(MessageFormat.CANAL, "tidb-canal", "15-rename-table-r2");
        assertEquals("RENAME TABLE `test`.`r2` TO `test`.`r2_new`", split2.getSql());
        handle(context, split2);
        assertNotNull(context.getDatabaseSchema().tableFor(new TableId("test", null, "r2_new")));

        handleStep(context, MessageFormat.CANAL, "tidb-canal", "16-alter-add-column");
        assertEquals(7, context.getDatabaseSchema().tableFor(after).columns().size());
        // a column added after the rename lands on the renamed table, not on a table of its own
        assertNotNull(context.getDatabaseSchema().tableFor(after).columnWithName("note"));
    }

    /**
     * Debezium 1.9.7 MySQL connector: the schema-history stream ({@code dbz-inventory}) plus the
     * data topics. A Debezium DDL record names its table only in {@code source.table}, and for a
     * multi-table statement that field holds a comma-joined list of names rather than one name.
     */
    @Test
    void testMysqlDebeziumSession() throws Exception {
        KafkaJsonSourceFetchTaskContext context =
                context(MessageFormat.DEBEZIUM, DdlParser.DEBEZIUM);
        TableId orders = new TableId("inventory", null, "orders");
        TableId archive = new TableId("inventory", null, "orders_archive");

        // a schema-change record whose DDL is a SET statement: no table, no schema change
        KafkaJsonMessage sessionSetup =
                message(MessageFormat.DEBEZIUM, "mysql-debezium", "00-schema-change-set-collation");
        assertNull(sessionSetup.getTable());
        assertNull(sessionSetup.getDatabase());
        handle(context, sessionSetup);
        assertTrue(drainAll(context.getQueue()).isEmpty());

        handleStep(
                context, MessageFormat.DEBEZIUM, "mysql-debezium", "01-schema-change-create-table");
        assertEquals(4, context.getDatabaseSchema().tableFor(orders).columns().size());
        drainAll(context.getQueue());

        assertEquals(
                1, convert(context, MessageFormat.DEBEZIUM, "mysql-debezium", "03-insert").size());
        assertEquals(
                1, convert(context, MessageFormat.DEBEZIUM, "mysql-debezium", "05-update").size());

        // the rename record does name a table -- and it is the new one
        KafkaJsonMessage rename =
                message(MessageFormat.DEBEZIUM, "mysql-debezium", "02-schema-change-rename-table");
        assertEquals(MessageType.DDL, rename.getMessageType());
        assertEquals("inventory", rename.getDatabase());
        assertEquals("orders_archive", rename.getTable());
        assertEquals("RENAME TABLE orders TO orders_archive", rename.getSql());
        handle(context, rename);
        assertNull(context.getDatabaseSchema().tableFor(orders));
        assertEquals(4, context.getDatabaseSchema().tableFor(archive).columns().size());
        assertEquals(
                "inventory.orders",
                renamePairs(context)
                        .get(0)
                        .asDocument()
                        .getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));

        // Debezium's data topic carries the renamed table's rows; with no mysqlType to fall back on
        // they decode against the registered schema alone
        assertEquals(
                1,
                convert(context, MessageFormat.DEBEZIUM, "mysql-debezium", "06-insert-after-rename")
                        .size());
        assertEquals(
                1,
                convert(context, MessageFormat.DEBEZIUM, "mysql-debezium", "07-update-after-rename")
                        .size());
        assertEquals(
                1,
                convert(context, MessageFormat.DEBEZIUM, "mysql-debezium", "08-delete-after-rename")
                        .size());

        handleStep(
                context,
                MessageFormat.DEBEZIUM,
                "mysql-debezium",
                "04-schema-change-alter-add-column");
        assertEquals(5, context.getDatabaseSchema().tableFor(archive).columns().size());

        handleStep(context, MessageFormat.DEBEZIUM, "mysql-debezium", "10-schema-change-create-t1");
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.DEBEZIUM, "mysql-debezium", "11-schema-change-create-t2");
        drainAll(context.getQueue());

        // one `RENAME TABLE t1 TO t1_new, t2 TO t2_new` is split into two records, and each names
        // its tables as the comma-joined list of the whole statement -- a value that is not a table
        // name at all, which is why both sides of the rename have to come from the DDL
        KafkaJsonMessage splitRename =
                message(
                        MessageFormat.DEBEZIUM,
                        "mysql-debezium",
                        "12-schema-change-rename-t1-split");
        assertEquals("t2_new,t1_new", splitRename.getTable());
        assertEquals("RENAME TABLE t1 TO t1_new", splitRename.getSql());
        handle(context, splitRename);
        assertNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1")));
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t1_new")));
        handleStep(
                context,
                MessageFormat.DEBEZIUM,
                "mysql-debezium",
                "13-schema-change-rename-t2-split");
        assertNotNull(
                context.getDatabaseSchema().tableFor(new TableId("inventory", null, "t2_new")));

        handleStep(context, MessageFormat.DEBEZIUM, "mysql-debezium", "14-schema-change-create-a");
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.DEBEZIUM, "mysql-debezium", "15-schema-change-create-b");
        drainAll(context.getQueue());

        // MySQL cannot swap two names in one statement (`RENAME TABLE x TO y, y TO x` fails with
        // "Table 'y' already exists"), so a swap goes through a temporary name -- and here it
        // arrives as three records, in the order that is itself safe to apply one at a time
        for (String step :
                new String[] {
                    "16-schema-change-rename-swap-1-a-to-a_tmp",
                    "17-schema-change-rename-swap-2-b-to-a",
                    "18-schema-change-rename-swap-3-a_tmp-to-b"
                }) {
            KafkaJsonMessage swap = message(MessageFormat.DEBEZIUM, "mysql-debezium", step);
            assertEquals("a,a_tmp,b", swap.getTable());
            handle(context, swap);
        }
        assertNotNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "a")));
        assertNotNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "b")));
        assertNull(context.getDatabaseSchema().tableFor(new TableId("inventory", null, "a_tmp")));
    }

    /**
     * The same canal-server producer, on the one statement the first session did not cover: the
     * temporary-name idiom {@code RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b}, which is how SQL
     * swaps two tables. A second session was captured for it because the first had already left the
     * statement behind.
     */
    @Test
    void testCanalServerSwapSession() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(MessageFormat.CANAL, DdlParser.DRUID);
        TableId a = new TableId("inventory", null, "a");
        TableId b = new TableId("inventory", null, "b");
        TableId temp = new TableId("inventory", null, "a_tmp");

        handleStep(
                context, MessageFormat.CANAL, "mysql-canal-swap", "01-session-reset-drop-database");
        handleStep(
                context,
                MessageFormat.CANAL,
                "mysql-canal-swap",
                "02-session-reset-create-database");
        assertTrue(drainAll(context.getQueue()).isEmpty());

        handleStep(context, MessageFormat.CANAL, "mysql-canal-swap", "03-create-table-a");
        assertEquals(2, context.getDatabaseSchema().tableFor(a).columns().size());
        drainAll(context.getQueue());
        handleStep(context, MessageFormat.CANAL, "mysql-canal-swap", "04-create-table-b");
        assertEquals(2, context.getDatabaseSchema().tableFor(b).columns().size());
        drainAll(context.getQueue());

        assertEquals(
                1, convert(context, MessageFormat.CANAL, "mysql-canal-swap", "06-insert-a").size());
        assertEquals(
                1, convert(context, MessageFormat.CANAL, "mysql-canal-swap", "08-insert-b").size());

        // Three pairs in one statement, and `table` announces the new name of the *first* one --
        // `a_tmp` is a name that is created and dropped by this very statement, so no message ever
        // announced it as a table of its own
        KafkaJsonMessage swap =
                message(MessageFormat.CANAL, "mysql-canal-swap", "09-rename-three-pair-swap");
        assertEquals(MessageType.DDL, swap.getMessageType());
        assertEquals("RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b", swap.getSql());
        assertEquals("a_tmp", swap.getTable());
        assertNull(context.getDatabaseSchema().tableFor(temp));
        handle(context, swap);

        // the statement is applied the way MySQL applies it, left to right, so the net effect is
        // the swap of a and b and nothing is left registered under the temporary name; the pair
        // that moves a_tmp on is folded into the pair that parked `a` there, because no consumer
        // can act on a name that never exists outside the statement
        Array pairs = renamePairs(context);
        assertEquals(2, pairs.size());
        assertEquals(
                "inventory.a",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.b",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
        assertEquals(
                "inventory.b",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "inventory.a",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
        assertNotNull(context.getDatabaseSchema().tableFor(a));
        assertNotNull(context.getDatabaseSchema().tableFor(b));
        assertNull(context.getDatabaseSchema().tableFor(temp));
    }

    /**
     * TiCDC's {@code debezium} protocol carries data changes only: the session that produced the
     * canal-json capture above wrote no schema-change record at all on this topic, so a connector
     * reading it never learns of a rename. The messages themselves are real, and they are asserted
     * to be DML-only here; the last part shows what that costs.
     */
    @Test
    void testTidbDebeziumSessionCarriesNoDdl() throws Exception {
        String[] session = {
            "01-insert-test_schema_change4",
            "02-update-test_schema_change4",
            "03-update-test_schema_change3",
            "04-insert-test_schema_change3",
            "05-delete-test_schema_change3",
            "06-insert-r1",
            "07-insert-r2"
        };
        for (String step : session) {
            KafkaJsonMessage message = message(MessageFormat.DEBEZIUM, "tidb-debezium", step);
            assertEquals(MessageType.DML, message.getMessageType(), step);
            assertNull(message.getSql(), step);
        }

        // the data records do carry the new name after the rename, which is exactly what a
        // connector that was never told about the rename cannot resolve
        KafkaJsonSourceFetchTaskContext context =
                context(MessageFormat.DEBEZIUM, DdlParser.DEBEZIUM);
        handleStep(
                context, MessageFormat.CANAL, "tidb-canal", "03-create-table-test_schema_change4");
        drainAll(context.getQueue());
        KafkaJsonMessage afterRename =
                message(MessageFormat.DEBEZIUM, "tidb-debezium", "03-update-test_schema_change3");
        assertEquals("test_schema_change3", afterRename.getTable());
        assertEquals(
                0,
                context.getRecordConverter()
                        .convert(afterRename, "kafka-json-topic", 0, KAFKA_OFFSET)
                        .size());
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    /** Reads a captured fixture as it arrived on the topic. */
    private static String fixture(String directory, String name) throws Exception {
        URI location =
                KafkaJsonCapturedMessageTest.class
                        .getClassLoader()
                        .getResource(ROOT + directory + "/" + name + ".json")
                        .toURI();
        return new String(Files.readAllBytes(Paths.get(location)), StandardCharsets.UTF_8);
    }

    private static KafkaJsonMessage message(MessageFormat format, String directory, String name)
            throws Exception {
        KafkaJsonMessage message =
                KafkaJsonParserFactory.create(format).parse(fixture(directory, name));
        assertNotNull(message, name);
        return message;
    }

    private static void handle(KafkaJsonSourceFetchTaskContext context, KafkaJsonMessage message)
            throws Exception {
        new KafkaJsonSchemaChangeHandler(context.getSourceConfig())
                .handle(context, message, offsetOf(message));
    }

    /** Parses one captured DDL message and drives it through the schema-change handler. */
    private static void handleStep(
            KafkaJsonSourceFetchTaskContext context,
            MessageFormat format,
            String directory,
            String name)
            throws Exception {
        handle(context, message(format, directory, name));
    }

    private static List<SourceRecord> convert(
            KafkaJsonSourceFetchTaskContext context,
            MessageFormat format,
            String directory,
            String name)
            throws Exception {
        return context.getRecordConverter()
                .convert(message(format, directory, name), "kafka-json-topic", 0, KAFKA_OFFSET);
    }

    /** Drains the queue and returns the rename pairs of the schema-change record it carried. */
    private static Array renamePairs(KafkaJsonSourceFetchTaskContext context) throws Exception {
        HistoryRecord historyRecord = historyRecord(context);
        assertEquals(
                KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE,
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertNotNull(pairs);
        return pairs;
    }

    private static HistoryRecord historyRecord(KafkaJsonSourceFetchTaskContext context)
            throws Exception {
        List<SourceRecord> records = drainAll(context.getQueue());
        assertFalse(records.isEmpty(), "expected a schema-change record");
        return SourceRecordUtils.getHistoryRecord(records.get(records.size() - 1));
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

    /**
     * Drains everything the queue currently holds: polls until a poll comes back empty, so that a
     * message enqueuing more than one record cannot leave the remainder behind for the next step.
     */
    private static List<SourceRecord> drainAll(ChangeEventQueue<DataChangeEvent> queue)
            throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        while (true) {
            List<SourceRecord> polled = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 1000L;
            do {
                for (DataChangeEvent event : queue.poll()) {
                    polled.add(event.getRecord());
                }
            } while (polled.isEmpty() && System.currentTimeMillis() < deadline);
            if (polled.isEmpty()) {
                return records;
            }
            records.addAll(polled);
        }
    }
}
