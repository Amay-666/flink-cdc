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

package org.apache.flink.cdc.connectors.canal.source.ddl;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * The {@link CanalDdlParser} implementation based on Debezium's MySQL ANTLR parser ({@link
 * MySqlAntlrDdlParser}).
 *
 * <p>The current schema, when known, is seeded into the {@link Tables} instance so that {@code
 * ALTER} statements are applied on top of it; after {@code parse} the affected table is read back
 * from the same instance. The change type is inferred from the seed: a table that existed before and
 * is gone afterwards was dropped, a table that did not exist and now does was created, anything else
 * on a known table is an alter.
 */
public class CanalDebeziumDdlParser implements CanalDdlParser {

    private static final Logger LOG = LoggerFactory.getLogger(CanalDebeziumDdlParser.class);

    private final MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
    private final Tables tables = new Tables();

    @Override
    public CanalDdlParsedResult parse(
            String database, TableId tableId, @Nullable Table currentTable, String ddl) {
        try {
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
                return new CanalDdlParsedResult(tableId, TableChangeType.CREATE, parsed);
            }
            if (parsed == null) {
                return new CanalDdlParsedResult(tableId, TableChangeType.DROP, null);
            }
            return new CanalDdlParsedResult(tableId, TableChangeType.ALTER, parsed);
        } catch (Exception e) {
            LOG.warn("Failed to parse DDL with the Debezium ANTLR parser: {}", ddl, e);
            return null;
        }
    }
}
