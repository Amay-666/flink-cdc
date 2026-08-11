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

import org.apache.flink.cdc.connectors.canal.source.utils.CanalTableUtils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The default {@link CanalDdlParser} implementation based on Alibaba Druid's MySQL SQL parser.
 *
 * <p>Handles {@code CREATE TABLE} (columns + primary key), {@code ALTER TABLE} ({@code ADD}/{@code
 * DROP}/{@code MODIFY}/{@code CHANGE} column, applied on top of the current schema) and {@code DROP
 * TABLE}. Statements that do not change the column model ({@code ADD INDEX}, {@code TRUNCATE},
 * {@code USE}, ...) yield {@code null} and are ignored. The column type mapping delegates to {@link
 * CanalTableUtils} so that a DDL-derived schema and a canal-message-derived schema are identical.
 */
public class CanalDruidDdlParser implements CanalDdlParser {

    private static final Logger LOG = LoggerFactory.getLogger(CanalDruidDdlParser.class);

    @Override
    public CanalDdlParsedResult parse(
            String database, TableId tableId, @Nullable Table currentTable, String ddl) {
        try {
            SQLStatement statement = SQLUtils.parseSingleMysqlStatement(ddl);
            if (statement instanceof SQLCreateTableStatement) {
                return parseCreate((SQLCreateTableStatement) statement, tableId);
            }
            if (statement instanceof SQLAlterTableStatement) {
                return parseAlter((SQLAlterTableStatement) statement, tableId, currentTable);
            }
            if (statement instanceof SQLDropTableStatement) {
                return new CanalDdlParsedResult(tableId, TableChangeType.DROP, null);
            }
            LOG.debug("Ignoring DDL statement that does not change the table schema: {}", ddl);
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to parse DDL with the Druid parser: {}", ddl, e);
            return null;
        }
    }

    private static CanalDdlParsedResult parseCreate(SQLCreateTableStatement statement, TableId tableId) {
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
        return new CanalDdlParsedResult(tableId, TableChangeType.CREATE, table.create());
    }

    @Nullable
    private static CanalDdlParsedResult parseAlter(
            SQLAlterTableStatement statement, TableId tableId, @Nullable Table currentTable) {
        if (currentTable == null) {
            LOG.warn(
                    "Skipping ALTER on {} without a known current schema: {}",
                    tableId,
                    statement);
            return null;
        }
        TableEditor table = currentTable.edit();
        for (SQLAlterTableItem item : statement.getItems()) {
            if (item instanceof SQLAlterTableAddColumn) {
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
            // other items (indexes, primary key, renames, ...) are not modeled by the Debezium
            // Table and are intentionally ignored
        }
        return new CanalDdlParsedResult(tableId, TableChangeType.ALTER, table.create());
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
     * expression (e.g. {@code varchar(255) unsigned}) and delegating to {@link CanalTableUtils}.
     */
    private static ColumnEditor buildColumn(SQLColumnDefinition column, int position) {
        return CanalTableUtils.buildColumn(
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
