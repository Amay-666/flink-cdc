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

import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTableUtils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The default {@link KafkaJsonDdlParser} implementation based on Alibaba Druid's MySQL SQL parser.
 *
 * <p>Handles {@code CREATE TABLE} (columns + primary key), {@code ALTER TABLE} ({@code ADD}/{@code
 * DROP}/{@code MODIFY}/{@code CHANGE} column, applied on top of the current schema), {@code DROP
 * TABLE} and the {@code RENAME TABLE}/{@code ALTER TABLE ... RENAME} table renames. A single-column
 * rename ({@code RENAME COLUMN}/{@code CHANGE}) is classified as {@link
 * KafkaJsonTableChangeType#RENAME_COLUMN}. Statements that do not change the column model ({@code ADD
 * INDEX}, {@code TRUNCATE}, {@code USE}, ...) yield {@code null} and are ignored. The column type
 * mapping delegates to {@link KafkaJsonTableUtils} so that a DDL-derived schema and a
 * canal-message-derived schema are identical.
 */
public class KafkaJsonDruidDdlParser implements KafkaJsonDdlParser {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDruidDdlParser.class);

    @Override
    public KafkaJsonDdlParsedResult parse(
            String database, TableId tableId, @Nullable Table currentTable, String ddl) {
        try {
            SQLStatement statement = SQLUtils.parseSingleMysqlStatement(ddl);
            if (statement instanceof SQLCreateTableStatement) {
                return parseCreate((SQLCreateTableStatement) statement, tableId);
            }
            if (statement instanceof MySqlRenameTableStatement) {
                return parseRenameTable(
                        (MySqlRenameTableStatement) statement, tableId, currentTable);
            }
            if (statement instanceof SQLAlterTableStatement) {
                return parseAlter((SQLAlterTableStatement) statement, tableId, currentTable);
            }
            if (statement instanceof SQLDropTableStatement) {
                return KafkaJsonDdlParsedResult.drop(tableId, currentTable);
            }
            LOG.debug("Ignoring DDL statement that does not change the table schema: {}", ddl);
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to parse DDL with the Druid parser: {}", ddl, e);
            return null;
        }
    }

    private static KafkaJsonDdlParsedResult parseCreate(SQLCreateTableStatement statement, TableId tableId) {
        TableEditor table = Table.editor().tableId(tableId);
        int position = 0;
        for (SQLTableElement element : statement.getTableElementList()) {
            if (element instanceof SQLColumnDefinition) {
                table.addColumn(buildColumn((SQLColumnDefinition) element, ++position).create());
            }
        }
        List<String> primaryKeyNames = statement.getPrimaryKeyNames();
        if (primaryKeyNames != null && !primaryKeyNames.isEmpty()) {
            table.setPrimaryKeyNames(primaryKeyNames);
        }
        return KafkaJsonDdlParsedResult.create(tableId, table.create());
    }

    /**
     * Parses a {@code RENAME TABLE a TO b} (the statement may rename several tables at once; only the
     * first pair is modeled, matching the single table announced by the canal message).
     */
    @Nullable
    private static KafkaJsonDdlParsedResult parseRenameTable(
            MySqlRenameTableStatement statement, TableId tableId, @Nullable Table currentTable) {
        List<MySqlRenameTableStatement.Item> items = statement.getItems();
        if (items == null || items.isEmpty()) {
            return null;
        }
        MySqlRenameTableStatement.Item item = items.get(0);
        if (item.getTo() == null) {
            return null;
        }
        TableId newTableId =
                new TableId(
                        tableId.catalog(), tableId.schema(), unquote(item.getTo().getSimpleName()));
        Table newTable =
                currentTable == null
                        ? null
                        : currentTable.edit().tableId(newTableId).create();
        return KafkaJsonDdlParsedResult.renameTable(tableId, newTableId, currentTable, newTable);
    }

    @Nullable
    private static KafkaJsonDdlParsedResult parseAlter(
            SQLAlterTableStatement statement, TableId tableId, @Nullable Table currentTable) {
        if (currentTable == null) {
            LOG.warn(
                    "Skipping ALTER on {} without a known current schema: {}",
                    tableId,
                    statement);
            return null;
        }
        TableEditor table = currentTable.edit();
        TableId effectiveTableId = tableId;
        for (SQLAlterTableItem item : statement.getItems()) {
            if (item instanceof SQLAlterTableRename) {
                // ALTER TABLE t RENAME [TO|AS] newname
                SQLName toName = ((SQLAlterTableRename) item).getToName();
                if (toName != null) {
                    effectiveTableId =
                            new TableId(
                                    tableId.catalog(),
                                    tableId.schema(),
                                    unquote(toName.getSimpleName()));
                    table.tableId(effectiveTableId);
                }
            } else if (item instanceof SQLAlterTableRenameColumn) {
                // ALTER TABLE t RENAME COLUMN a TO b
                SQLAlterTableRenameColumn rename = (SQLAlterTableRenameColumn) item;
                if (rename.getColumn() != null && rename.getTo() != null) {
                    String oldName = unquote(rename.getColumn().getSimpleName());
                    String newName = unquote(rename.getTo().getSimpleName());
                    Column oldColumn = table.columnWithName(oldName);
                    if (oldColumn != null) {
                        int position = positionOf(table, oldName);
                        table.removeColumn(oldName);
                        table.addColumn(
                                oldColumn.edit().name(newName).position(position).create());
                    }
                }
            } else if (item instanceof SQLAlterTableAddColumn) {
                for (SQLColumnDefinition column : ((SQLAlterTableAddColumn) item).getColumns()) {
                    table.addColumn(buildColumn(column, table.columns().size() + 1).create());
                }
            } else if (item instanceof SQLAlterTableDropColumnItem) {
                for (SQLName column : ((SQLAlterTableDropColumnItem) item).getColumns()) {
                    table.removeColumn(unquote(column.getSimpleName()));
                }
            } else if (item instanceof MySqlAlterTableModifyColumn) {
                SQLColumnDefinition column =
                        ((MySqlAlterTableModifyColumn) item).getNewColumnDefinition();
                table.updateColumn(
                        buildColumn(column, positionOf(table, unquote(column.getColumnName())))
                                .create());
            } else if (item instanceof MySqlAlterTableChangeColumn) {
                MySqlAlterTableChangeColumn change = (MySqlAlterTableChangeColumn) item;
                String oldName = unquote(change.getColumnName().getSimpleName());
                SQLColumnDefinition newColumn = change.getNewColumnDefinition();
                int position = positionOf(table, oldName);
                table.removeColumn(oldName);
                table.addColumn(buildColumn(newColumn, position).create());
            }
            // other items (indexes, primary key, ...) are not modeled by the Debezium
            // Table and are intentionally ignored
        }
        Table newTable = table.create();
        if (!effectiveTableId.equals(tableId)) {
            return KafkaJsonDdlParsedResult.renameTable(
                    tableId, effectiveTableId, currentTable, newTable);
        }
        if (KafkaJsonDdlParsedResult.isPureColumnRename(currentTable, newTable)) {
            return KafkaJsonDdlParsedResult.renameColumn(tableId, currentTable, newTable);
        }
        return KafkaJsonDdlParsedResult.alter(tableId, currentTable, newTable);
    }

    private static int positionOf(TableEditor table, String columnName) {
        for (Column column : table.columns()) {
            if (column.name().equals(columnName)) {
                return column.position();
            }
        }
        return table.columns().size() + 1;
    }

    /**
     * Builds a {@link ColumnEditor} from a Druid column definition by reconstructing the MySQL type
     * expression (e.g. {@code varchar(255) unsigned}) and delegating to {@link KafkaJsonTableUtils}.
     */
    private static ColumnEditor buildColumn(SQLColumnDefinition column, int position) {
        return KafkaJsonTableUtils.buildColumn(
                unquote(column.getColumnName()), mysqlTypeExpression(column.getDataType()), position);
    }

    /** Strips the surrounding backticks Druid keeps on quoted identifiers (e.g. {@code `id`}). */
    private static String unquote(String name) {
        if (name != null && name.length() >= 2 && name.startsWith("`") && name.endsWith("`")) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    private static String mysqlTypeExpression(SQLDataType dataType) {
        StringBuilder expression = new StringBuilder(dataType.getName().toLowerCase(Locale.ROOT));
        List<SQLExpr> arguments = dataType.getArguments();
        if (arguments != null && !arguments.isEmpty()) {
            expression
                    .append('(')
                    .append(arguments.stream().map(SQLExpr::toString).collect(Collectors.joining(",")))
                    .append(')');
        }
        if (dataType instanceof SQLDataTypeImpl) {
            // the unsigned/zerofill flags are not part of getName() but exposed by the impl
            SQLDataTypeImpl impl = (SQLDataTypeImpl) dataType;
            if (impl.isUnsigned()) {
                expression.append(" unsigned");
            }
            if (impl.isZerofill()) {
                expression.append(" zerofill");
            }
        }
        return expression.toString();
    }
}
