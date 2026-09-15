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

package org.apache.flink.cdc.connectors.kafkajson.source.handler;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDdlParsedResult;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDebeziumDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDruidDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonRenamePair;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonTableChangeType;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonPartition;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfoStructMaker;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles the DDL messages of the canal stream: applies the schema change to the shared {@code
 * KafkaJsonSchema} — so that subsequent data records of the affected table are decoded with the new
 * schema — and, when {@code include.schema.changes} is enabled, enqueues the Debezium-shaped
 * schema-change {@link SourceRecord} into the shared queue.
 *
 * <p>The schema-change record is built here directly instead of going through {@code
 * JdbcSourceEventDispatcher.dispatchSchemaChangeEvent}: the dispatcher's {@code
 * SchemaChangeEventReceiver} only enqueues the record when {@code
 * CommonConnectorConfig.isSchemaChangesHistoryEnabled()} is true, which the bundled Debezium 1.9.8
 * hard-codes to {@code false} for the MySQL connector. The record mirrors the dispatcher's format
 * so that the base {@code IncrementalSourceRecordEmitter} — which reads the history record for the
 * stream-split schema bookkeeping — consumes it unchanged.
 */
public class KafkaJsonSchemaChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSchemaChangeHandler.class);

    private static final String HISTORY_RECORD_FIELD = "historyRecord";
    private static final String SCHEMA_CHANGE_KEY_NAME =
            "io.debezium.connector.kafka.json.SchemaChangeKey";
    private static final String SCHEMA_CHANGE_VALUE_NAME =
            "io.debezium.connector.kafka.json.SchemaChangeValue";

    /**
     * The custom fields set on the history-record document of a rename/truncate schema change. They
     * are read by {@code KafkaJsonEventDeserializer} (in the pipeline module) to rebuild the rename
     * or truncate that the Debezium {@code TableChanges.TableChangeType} cannot express: {@code
     * RENAME_TABLE} carries its pairs in {@link #RENAME_PAIRS}, {@code RENAME_COLUMN} only the type
     * marker, {@code TRUNCATE_TABLE} only the type marker.
     */
    public static final String TABLE_CHANGE_TYPE = "tableChangeType";

    public static final String TABLE_CHANGE_TYPE_RENAME_TABLE = "RENAME_TABLE";
    public static final String TABLE_CHANGE_TYPE_RENAME_COLUMN = "RENAME_COLUMN";
    public static final String TABLE_CHANGE_TYPE_TRUNCATE_TABLE = "TRUNCATE_TABLE";
    public static final String TABLE_CHANGE_TYPE_ALTER_COLUMN_TYPE = "ALTER_COLUMN_TYPE";
    public static final String TABLE_CHANGE_TYPE_ALTER_COLUMN_COMMENT = "ALTER_COLUMN_COMMENT";

    /**
     * The custom history-record field carrying the renames of a {@code RENAME_TABLE} as an array of
     * {@code {oldTableId, newTableId}} documents: one per table the statement moved, in statement
     * order, with the temporary names of a swap folded into the rename that started them (see
     * {@link #applyRename}).
     *
     * <p>Both sides of every pair come from the DDL statement, never from the message: a canal
     * message announces the <b>post</b>-rename table name, so the announced name is the new side of
     * the pair and the old side appears nowhere but in the SQL. The pairs stay together because the
     * statement applied them together — and because a downstream that has to decide between a plain
     * {@code ALTER TABLE ... RENAME} and a name swap (Doris) can only tell whether the names form a
     * cycle when it sees all of them at once.
     */
    public static final String RENAME_PAIRS = "renamePairs";

    /** The {@code oldTableId} field of one element of {@link #RENAME_PAIRS}. */
    public static final String OLD_TABLE_ID = "oldTableId";

    /** The {@code newTableId} field of one element of {@link #RENAME_PAIRS}. */
    public static final String NEW_TABLE_ID = "newTableId";

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    private final KafkaJsonDdlParser ddlParser;
    private final KafkaJsonSourceInfoStructMaker sourceInfoStructMaker;

    public KafkaJsonSchemaChangeHandler(KafkaJsonSourceConfig sourceConfig) {
        this.ddlParser = createParser(sourceConfig.getDdlParser());
        this.sourceInfoStructMaker =
                new KafkaJsonSourceInfoStructMaker(
                        "kafka.json",
                        KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION,
                        sourceConfig.getDbzConnectorConfig());
    }

    private static KafkaJsonDdlParser createParser(DdlParser parserType) {
        switch (parserType) {
            case DEBEZIUM:
                return new KafkaJsonDebeziumDdlParser();
            case DRUID:
            default:
                return new KafkaJsonDruidDdlParser();
        }
    }

    /**
     * Applies the DDL of the message to the shared schema and enqueues the schema-change record.
     *
     * @param context the fetch task context (shared schema + queue)
     * @param message the DDL message (canal flatMessage or Debezium schema-change record)
     * @param offset the stream position of the message
     */
    public void handle(
            KafkaJsonSourceFetchTaskContext context,
            KafkaJsonMessage message,
            KafkaJsonOffset offset)
            throws IOException, InterruptedException {
        String sql = message.getSql();
        if (sql == null || sql.isEmpty()) {
            return;
        }
        // The announced table name is only a hint: it is the *pre*-change name for CREATE / ALTER /
        // DROP, but a canal message announces the *post*-rename name for a rename, and a Debezium
        // schema-change record announces no name at all (`table` is null, and a TableId cannot hold
        // a null table name). Both DDL parsers therefore take the tables of a rename from the
        // statement itself, and this lookup is only what seeds an ALTER.
        TableId announcedTableId =
                message.getTable() == null
                        ? null
                        : new TableId(
                                message.getDatabase() == null ? "" : message.getDatabase(),
                                null,
                                message.getTable());
        Table currentTable =
                announcedTableId == null
                        ? null
                        : context.getDatabaseSchema().tableFor(announcedTableId);
        KafkaJsonDdlParsedResult result =
                ddlParser.parse(message.getDatabase(), announcedTableId, currentTable, sql);
        if (result == null) {
            LOG.debug("Skipping DDL that does not change the table schema: {}", sql);
            return;
        }
        if (result.getType() != KafkaJsonTableChangeType.RENAME_TABLE
                && result.getTableId() == null) {
            // The message named no table and the statement is neither a rename (whose table names
            // come from the SQL) nor attributable to one, so applying it would register or drop a
            // table with no name.
            LOG.warn("Skipping a DDL that cannot be attributed to a table: {}", sql);
            return;
        }
        List<KafkaJsonRenamePair> renamePairs = applySchemaChange(context, result);
        if (context.getSourceConfig().isIncludeSchemaChanges()) {
            enqueueSchemaChange(context, message, offset, result, renamePairs);
        }
    }

    /**
     * Applies the parsed change to the shared schema and returns the rename pairs it resolved (an
     * empty list for every other change type).
     */
    private static List<KafkaJsonRenamePair> applySchemaChange(
            KafkaJsonSourceFetchTaskContext context, KafkaJsonDdlParsedResult result) {
        KafkaJsonTableChangeType type = result.getType();
        if (type == KafkaJsonTableChangeType.DROP) {
            context.getDatabaseSchema().removeTable(result.getTableId());
            return Collections.emptyList();
        }
        if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            return applyRename(context, result);
        }
        if (result.getNewTable() != null) {
            // CREATE / ALTER / RENAME_COLUMN all leave the affected table under the same id
            context.getDatabaseSchema().registerTable(result.getNewTable());
        }
        return Collections.emptyList();
    }

    /**
     * Applies the pairs of a rename to the shared registry in statement order, resolving each
     * pair's pre-rename schema as it goes, and returns the pairs that a downstream has to act on:
     * one per table the statement moved, with the temporary names composed away.
     *
     * <p>The pairs are applied one after another, exactly as the source engine applies them: each
     * one moves its source table onto its target, and the next sees the state the previous one
     * left. That order is what makes the temporary name of a swap resolvable — {@code RENAME TABLE
     * a TO a_tmp, b TO a, a_tmp TO b} moves {@code a} onto a name that exists only inside the
     * statement and moves it on again in the last pair, so afterwards nothing is left under it. The
     * idiom is the only way to swap two tables, because the direct {@code RENAME TABLE a TO b, b TO
     * a} is refused by MySQL and TiDB with {@code ERROR 1050 Table 'b' already exists} — verified
     * on MySQL 8.0, which also refuses the shorter chain {@code RENAME TABLE a TO b, b TO c}.
     * Because a target may therefore never be a name that already exists, no pair can overwrite
     * another's registration, and no interleaving is needed.
     *
     * <p>A pair whose source is a name an earlier pair of the same statement produced does not
     * rename a table of its own: it moves on the very table that pair moved aside, and the name in
     * between never exists outside the statement. Such a pair is folded into the pair that started
     * the chain, so the returned pairs name only tables that exist on both sides of the statement —
     * which is what a downstream needs. A consumer cannot act on the transient name: Doris would be
     * asked to rename a table it has never seen ({@code ALTER TABLE a_tmp RENAME b} after the swap
     * idiom) and would leave a stray table under the temporary name, where the composed pair
     * instead reports the plain exchange of {@code a} and {@code b} and lets the sink express it as
     * one atomic statement. The statement itself is not lost — it travels in the schema-change
     * record's DDL field.
     *
     * <p>Both schemas of each returned pair come from the pair's own old table id rather than from
     * the message, which is what makes the lookup hit at all for a canal message (it announces the
     * new name). A miss means the rename was announced for a table whose {@code CREATE} this
     * connector never saw — in that case the renamed table would be registered with an empty schema
     * and every following data record of it would be decoded as a row of zero columns, so it is
     * reported instead.
     */
    private static List<KafkaJsonRenamePair> applyRename(
            KafkaJsonSourceFetchTaskContext context, KafkaJsonDdlParsedResult result) {
        List<KafkaJsonRenamePair> resolved = new ArrayList<>(result.getRenamePairs().size());
        // The name a pair of this statement left a table under -> the position of that table's pair
        // in `resolved`. An entry means the name currently belongs to a rename already reported,
        // so a pair starting from it continues that rename rather than starting a new one.
        Map<String, Integer> chainPosition = new HashMap<>();
        for (KafkaJsonRenamePair pair : result.getRenamePairs()) {
            Table oldTable = context.getDatabaseSchema().tableFor(pair.getOldTableId());
            if (oldTable == null) {
                throw new IllegalStateException(
                        "Cannot apply the rename of "
                                + pair.getOldTableId()
                                + " to "
                                + pair.getNewTableId()
                                + ": the schema of the table being renamed was never observed, so "
                                + "the renamed table would be registered without any columns. Start "
                                + "the job (or its Kafka start offset) at a point where the CREATE "
                                + "of that table is seen before the rename.");
            }
            Table newTable =
                    pair.getNewTable() != null
                            ? pair.getNewTable()
                            : oldTable.edit().tableId(pair.getNewTableId()).create();
            context.getDatabaseSchema().removeTable(pair.getOldTableId());
            context.getDatabaseSchema().registerTable(newTable);
            Integer chainedFrom = chainPosition.remove(pair.getOldTableId().toString());
            if (chainedFrom == null) {
                chainPosition.put(pair.getNewTableId().toString(), resolved.size());
                resolved.add(pair.withSchemas(oldTable, newTable));
            } else {
                // The table of pair `chainedFrom` has just moved on: it keeps its pre-statement
                // schema and its original id, and its target name becomes the one of this pair.
                KafkaJsonRenamePair origin = resolved.get(chainedFrom);
                chainPosition.put(pair.getNewTableId().toString(), chainedFrom);
                resolved.set(
                        chainedFrom,
                        new KafkaJsonRenamePair(
                                origin.getOldTableId(),
                                pair.getNewTableId(),
                                origin.getOldTable(),
                                newTable));
            }
        }
        return resolved;
    }

    /**
     * The {@code es}/{@code ts} event times of a message, per format (Debezium has neither field).
     */
    private static long[] eventTimesOf(KafkaJsonMessage message) {
        if (message instanceof DebeziumMessage) {
            DebeziumMessage dbz = (DebeziumMessage) message;
            return new long[] {dbz.getEs(), dbz.getTs()};
        }
        CanalMessage flat = (CanalMessage) message;
        return new long[] {flat.getEs(), flat.getTs()};
    }

    private void enqueueSchemaChange(
            KafkaJsonSourceFetchTaskContext context,
            KafkaJsonMessage message,
            KafkaJsonOffset offset,
            KafkaJsonDdlParsedResult result,
            List<KafkaJsonRenamePair> renamePairs)
            throws IOException, InterruptedException {
        KafkaJsonSourceConfig sourceConfig = context.getSourceConfig();
        MySqlConnectorConfig dbzConfig = sourceConfig.getDbzConnectorConfig();

        Schema keySchema =
                SchemaBuilder.struct()
                        .name(SchemaNameAdjuster.create().adjust(SCHEMA_CHANGE_KEY_NAME))
                        .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name(SchemaNameAdjuster.create().adjust(SCHEMA_CHANGE_VALUE_NAME))
                        .field(HistoryRecord.Fields.SOURCE, sourceInfoStructMaker.schema())
                        .field(HISTORY_RECORD_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();

        TableChanges tableChanges = new TableChanges();
        KafkaJsonTableChangeType type = result.getType();
        if (type == KafkaJsonTableChangeType.CREATE) {
            tableChanges.create(result.getNewTable());
        } else if (type == KafkaJsonTableChangeType.ALTER
                || type == KafkaJsonTableChangeType.RENAME_COLUMN
                || type == KafkaJsonTableChangeType.ALTER_COLUMN_TYPE
                || type == KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT) {
            // The Debezium history format carries only the post-change schema in an ALTER
            // TableChange. The pipeline deserializer derives the column-level events by diffing the
            // old and the new schema, so it needs both images: snapshot tables announce their
            // schema
            // as CreateTableEvents via JDBC (bypassing the schema-change stream), so the
            // deserializer
            // never observed their CREATE and cannot diff an ALTER on its own. Carry the pre-change
            // schema as a leading ALTER change — the deserializer processes the changes in order,
            // so
            // the leading change primes its registry and the trailing change is diffed against it.
            if (result.getOldTable() != null) {
                tableChanges.alter(result.getOldTable());
            }
            tableChanges.alter(result.getNewTable());
        } else if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            // The schema of each renamed table travels as a CREATE change of its new table id, in
            // the same order as the pairs; the rename itself is carried by the custom
            // history-record fields below.
            for (KafkaJsonRenamePair pair : renamePairs) {
                tableChanges.create(pair.getNewTable());
            }
        } else if (type == KafkaJsonTableChangeType.TRUNCATE) {
            // TRUNCATE does not change the schema: the type marker below is what announces the
            // truncate to the pipeline deserializer, which reads the (unchanged) table schema from
            // its own registry and attaches it to the TruncateTableEvent.
        } else if (type == KafkaJsonTableChangeType.DROP) {
            // the serializer requires a (possibly empty) table for a DROP change
            tableChanges.drop(Table.editor().tableId(result.getTableId()).create());
        }

        Map<String, Object> source = new HashMap<>();
        source.put(AbstractSourceInfo.DATABASE_NAME_KEY, message.getDatabase());
        long[] eventTimes = eventTimesOf(message);
        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
                        dbzConfig,
                        message.getDatabase(),
                        message.getTable(),
                        offset.getEventTime(),
                        eventTimes[0],
                        eventTimes[1],
                        SnapshotRecord.FALSE);
        Struct sourceStruct = sourceInfoStructMaker.struct(sourceInfo);

        HistoryRecord historyRecord =
                new HistoryRecord(
                        source,
                        offset.getOffset(),
                        message.getDatabase(),
                        null,
                        message.getSql(),
                        tableChanges);
        Document historyDocument = historyRecord.document();
        if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_RENAME_TABLE);
            historyDocument.set(RENAME_PAIRS, renamePairsDocument(renamePairs));
        } else if (type == KafkaJsonTableChangeType.RENAME_COLUMN) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_RENAME_COLUMN);
        } else if (type == KafkaJsonTableChangeType.TRUNCATE) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_TRUNCATE_TABLE);
        } else if (type == KafkaJsonTableChangeType.ALTER_COLUMN_TYPE) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_ALTER_COLUMN_TYPE);
        } else if (type == KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_ALTER_COLUMN_COMMENT);
        }
        String historyStr = DOCUMENT_WRITER.write(historyDocument);

        Struct key = new Struct(keySchema);
        key.put(HistoryRecord.Fields.DATABASE_NAME, message.getDatabase());

        Struct value = new Struct(valueSchema);
        value.put(HistoryRecord.Fields.SOURCE, sourceStruct);
        value.put(HISTORY_RECORD_FIELD, historyStr);

        String topic = MySqlTopicSelector.defaultSelector(dbzConfig).getPrimaryTopic();
        SourceRecord record =
                new SourceRecord(
                        new KafkaJsonPartition(dbzConfig.getLogicalName()).getSourcePartition(),
                        offset.getOffset(),
                        topic,
                        0,
                        keySchema,
                        key,
                        valueSchema,
                        value);
        context.getQueue().enqueue(new DataChangeEvent(record));
    }

    /** Returns the {@link KafkaJsonDdlParser} used by this handler (exposed for unit tests). */
    public KafkaJsonDdlParser getDdlParser() {
        return ddlParser;
    }

    /**
     * Serializes the rename pairs into the {@link #RENAME_PAIRS} array of the history-record
     * document, preserving statement order.
     */
    private static Array renamePairsDocument(List<KafkaJsonRenamePair> pairs) {
        Array array = Array.create();
        for (KafkaJsonRenamePair pair : pairs) {
            Document pairDocument = Document.create();
            pairDocument.set(OLD_TABLE_ID, pair.getOldTableId().toString());
            pairDocument.set(NEW_TABLE_ID, pair.getNewTableId().toString());
            array.add(pairDocument);
        }
        return array;
    }
}
