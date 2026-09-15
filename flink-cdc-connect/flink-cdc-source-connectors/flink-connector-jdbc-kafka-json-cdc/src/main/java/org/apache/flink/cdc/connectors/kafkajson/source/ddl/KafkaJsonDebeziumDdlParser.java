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

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@link KafkaJsonDdlParser} implementation based on Debezium's MySQL ANTLR parser ({@link
 * MySqlAntlrDdlParser}).
 *
 * <p>The current schema, when known, is seeded into the {@link Tables} instance so that {@code
 * ALTER} statements are applied on top of it; after {@code parse} the affected table is read back
 * from the same instance. The change type is inferred from the seed: a table that existed before
 * and is gone afterwards without any replacement was dropped, a table that did not exist and now
 * does was created, anything else on a known table is an alter — or a single-column rename, when
 * the two schemas only differ in one column name.
 *
 * <p>Table renames are the exception: they are read from the statement with Druid ({@link
 * KafkaJsonRenameTableIds}) instead of being inferred from the seeded tables. The ANTLR parser
 * cannot even apply a rename whose pre-rename schema was never seeded, and inferring it from "the
 * announced table id disappeared and exactly one new one appeared" recognizes neither the several
 * pairs of one {@code RENAME TABLE} nor an {@code a TO b, b TO a} swap — both would degrade to a
 * drop. The inference is kept as a fallback below for a rename Druid cannot parse.
 */
public class KafkaJsonDebeziumDdlParser implements KafkaJsonDdlParser {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDebeziumDdlParser.class);

    private final MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
    private final Tables tables = new Tables();

    @Override
    public KafkaJsonDdlParsedResult parse(
            String database, @Nullable TableId tableId, @Nullable Table currentTable, String ddl) {
        try {
            List<KafkaJsonRenamePair> renamePairs = parseRenameIds(database, ddl);
            if (renamePairs != null) {
                return KafkaJsonDdlParsedResult.renameTable(
                        KafkaJsonDdlParsedResult.resolveSchemas(
                                renamePairs,
                                oldTableId -> oldTableId.equals(tableId) ? currentTable : null));
            }
            if (tableId == null) {
                // No id to look the change up under: a Debezium schema-change record names its
                // database and its DDL but no table, and the ANTLR parse below is read back by
                // table
                // id. Diffing the parsed tables instead would recognize neither a multi-pair rename
                // nor an a-to-b, b-to-a swap, so the change is reported as no change.
                return null;
            }
            tables.clear();
            if (currentTable != null) {
                tables.overwriteTable(currentTable);
            }
            parser.setCurrentDatabase(database);
            parser.parse(ddl, tables);
            Table parsed = tables.forTable(tableId);
            if (currentTable == null) {
                if (parsed == null) {
                    return null;
                }
                return KafkaJsonDdlParsedResult.create(tableId, parsed);
            }
            if (parsed == null) {
                // The announced table is gone: either it was renamed to another table id or
                // dropped.
                TableId renamedTo = findRenamedTable(tableId, tables.tableIds());
                if (renamedTo != null) {
                    return KafkaJsonDdlParsedResult.renameTable(
                            tableId, renamedTo, currentTable, tables.forTable(renamedTo));
                }
                return KafkaJsonDdlParsedResult.drop(tableId, currentTable);
            }
            if (isTruncateStatement(ddl, parsed, currentTable)) {
                return KafkaJsonDdlParsedResult.truncate(tableId, currentTable);
            }
            if (KafkaJsonDdlParsedResult.isPureColumnRename(currentTable, parsed)) {
                return KafkaJsonDdlParsedResult.renameColumn(tableId, currentTable, parsed);
            }
            return KafkaJsonDdlParsedResult.alter(tableId, currentTable, parsed);
        } catch (Exception e) {
            LOG.warn("Failed to parse DDL with the Debezium ANTLR parser: {}", ddl, e);
            return null;
        }
    }

    /** Returns whether the DDL is a TRUNCATE statement and the schema is unchanged. */
    private static boolean isTruncateStatement(String ddl, Table parsed, Table currentTable) {
        return parsed.equals(currentTable)
                && ddl.trim().toUpperCase(java.util.Locale.ROOT).startsWith("TRUNCATE");
    }

    /**
     * Returns the rename pairs of the statement, or {@code null} when it is not a rename this
     * parser recognizes. The name resolution lives in {@link KafkaJsonRenameTableIds}, which both
     * DDL parsers share; a statement Druid cannot parse falls through to the ANTLR path below
     * rather than being reported as a rename.
     */
    @Nullable
    private static List<KafkaJsonRenamePair> parseRenameIds(String database, String ddl) {
        SQLStatement statement;
        try {
            statement = SQLUtils.parseSingleMysqlStatement(ddl);
        } catch (Exception e) {
            LOG.debug("Druid cannot parse this DDL, using the ANTLR parser instead: {}", ddl, e);
            return null;
        }
        if (!KafkaJsonRenameTableIds.isRename(statement)) {
            return null;
        }
        return KafkaJsonRenameTableIds.extract(statement, database);
    }

    /**
     * Returns the id of the table {@code tableId} was renamed to, or {@code null} when the
     * announced table was dropped instead: a table id present after the parse but not among the
     * seeded current schema. This is the fallback for a rename {@link #parseRenameIds} does not
     * recognize, and only a single replacement is recognized (a statement renaming several tables
     * at once falls back to a {@code DROP}).
     */
    @Nullable
    private static TableId findRenamedTable(TableId tableId, Set<TableId> tableIds) {
        Set<TableId> before = Collections.singleton(tableId);
        List<TableId> newIds =
                tableIds.stream().filter(id -> !before.contains(id)).collect(Collectors.toList());
        return newIds.size() == 1 ? newIds.get(0) : null;
    }
}
