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

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Extracts the {@code old TO new} table ids of a rename statement from a Druid AST.
 *
 * <p>Both {@link KafkaJsonDdlParser} implementations route renames through here, because the table
 * name a message announces is unreliable for a rename:
 *
 * <ul>
 *   <li>a canal message carries the <b>new</b> name in its {@code table} field (TiCDC writes the
 *       post-rename name), so looking the pre-rename schema up under it always misses;
 *   <li>a Debezium schema-change record carries no table name at all ({@code table} is null).
 * </ul>
 *
 * <p>The DDL statement is the only place the pre-rename names appear, so it is the authoritative
 * source for both sides of every pair. The Debezium ANTLR parser cannot infer them by diffing the
 * parsed tables either — that only ever recognizes a single replacement, and a statement renaming
 * several tables at once (including an {@code a TO b, b TO a} swap) would degrade to a drop.
 */
final class KafkaJsonRenameTableIds {

    private KafkaJsonRenameTableIds() {}

    /** Returns whether the statement renames at least one table. */
    static boolean isRename(SQLStatement statement) {
        return statement instanceof MySqlRenameTableStatement
                || (statement instanceof SQLAlterTableStatement
                        && renameItemOf((SQLAlterTableStatement) statement) != null);
    }

    /**
     * Returns the {@code old TO new} ids of a rename statement in statement order, or {@code null}
     * when the statement is not a rename (or names a table without a usable name).
     *
     * @param defaultDatabase the database to qualify an unqualified table name with
     */
    @Nullable
    static List<KafkaJsonRenamePair> extract(SQLStatement statement, String defaultDatabase) {
        if (statement instanceof MySqlRenameTableStatement) {
            List<MySqlRenameTableStatement.Item> items =
                    ((MySqlRenameTableStatement) statement).getItems();
            if (items == null || items.isEmpty()) {
                return null;
            }
            List<KafkaJsonRenamePair> pairs = new ArrayList<>(items.size());
            for (MySqlRenameTableStatement.Item item : items) {
                KafkaJsonRenamePair pair = pairOf(item.getName(), item.getTo(), defaultDatabase);
                if (pair == null) {
                    // A pair we cannot name is not a pair we can apply: refuse the whole
                    // statement rather than renaming a subset of it.
                    return null;
                }
                pairs.add(pair);
            }
            return pairs;
        }
        if (statement instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement alter = (SQLAlterTableStatement) statement;
            SQLAlterTableRename rename = renameItemOf(alter);
            if (rename == null) {
                return null;
            }
            KafkaJsonRenamePair pair = pairOf(alter.getName(), rename.getToName(), defaultDatabase);
            return pair == null ? null : Collections.singletonList(pair);
        }
        return null;
    }

    /** Returns the {@code RENAME [TO|AS]} item of an {@code ALTER TABLE}, or {@code null}. */
    @Nullable
    private static SQLAlterTableRename renameItemOf(SQLAlterTableStatement alter) {
        List<SQLAlterTableItem> items = alter.getItems();
        if (items == null) {
            return null;
        }
        for (SQLAlterTableItem item : items) {
            if (item instanceof SQLAlterTableRename) {
                return (SQLAlterTableRename) item;
            }
        }
        return null;
    }

    @Nullable
    private static KafkaJsonRenamePair pairOf(
            @Nullable SQLName from, @Nullable SQLName to, String defaultDatabase) {
        TableId oldTableId = tableIdOf(from, defaultDatabase);
        TableId newTableId = tableIdOf(to, defaultDatabase);
        return oldTableId == null || newTableId == null
                ? null
                : KafkaJsonRenamePair.of(oldTableId, newTableId);
    }

    /**
     * Builds a {@link TableId} from a (possibly qualified) SQL name, defaulting the database to
     * {@code defaultDatabase} when the name carries no qualifier. A cross-database rename ({@code
     * RENAME TABLE db1.a TO db2.b}) is therefore represented faithfully — and rejected downstream,
     * where the sink cannot express it.
     */
    @Nullable
    private static TableId tableIdOf(@Nullable SQLName name, String defaultDatabase) {
        if (name == null) {
            return null;
        }
        String table;
        String database = defaultDatabase;
        if (name instanceof SQLPropertyExpr) {
            SQLPropertyExpr qualified = (SQLPropertyExpr) name;
            table = unquote(qualified.getName());
            SQLExpr owner = qualified.getOwner();
            if (owner instanceof SQLIdentifierExpr) {
                database = unquote(((SQLIdentifierExpr) owner).getName());
            }
        } else {
            table = unquote(name.getSimpleName());
        }
        if (table == null || table.isEmpty()) {
            return null;
        }
        return new TableId(database, null, table);
    }

    /** Strips the surrounding backticks Druid keeps on quoted identifiers (e.g. {@code `id`}). */
    private static String unquote(String name) {
        if (name != null && name.length() >= 2 && name.startsWith("`") && name.endsWith("`")) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }
}
