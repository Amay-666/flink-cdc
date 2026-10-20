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

import org.apache.flink.cdc.connectors.kafkajson.source.type.MySqlEnumSetLabels;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link JdbcConnection} that compensates the JDBC driver's column-metadata quirks where the
 * metadata is read, so that every table it builds matches the shared type mapping the canal-message
 * and Druid-DDL paths build:
 *
 * <ul>
 *   <li>{@link #overrideColumn(ColumnEditor)} remaps TINYINT to SMALLINT and clears the meaningless
 *       scale of approximate numerics — the driver-reported TINYINT jdbcType maps to an INT8 schema
 *       the unsigned value no longer fits, and TiDB's FLOAT DECIMAL_DIGITS=0 suppresses the
 *       single-precision FLOAT32 branch;
 *   <li>the {@link #readSchema(Tables, String, String, Tables.TableFilter, ColumnNameFilter,
 *       boolean)} table loop fills the ENUM/SET label lists — from the TYPE_NAME itself when it
 *       carries the values (MySQL proper reports {@code enum('a','b','c')}), otherwise from {@code
 *       information_schema.COLUMNS}'s COLUMN_TYPE (TiDB reports the TYPE_NAME bare, "ENUM"/"SET").
 *       Without the labels, TiCDC's numeric ENUM index / SET bitmask cannot be mapped back to their
 *       text (see {@code KafkaJsonValueConverter#convert}); later value changes keep the labels
 *       fresh through the schema-change path, whose DDL text carries the whole new list.
 * </ul>
 *
 * <p>The column read also skips the {@code COLUMN_DEF} default value: {@link
 * JdbcConnection#readTableColumn} fetches it ({@code getString(13)}) and feeds it into Debezium's
 * {@code TableSchemaBuilder} as the {@code defaultValueExpression}, but for binary-typed columns
 * the MySQL driver reports the default as a literal string ({@code 0x} for a {@code BINARY DEFAULT
 * 0x...}, or raw binary bytes on TiDB), which {@code SchemaBuilder.defaultValue} refuses to convert
 * into a {@code BYTES} default and aborts the whole schema read with {@code DebeziumException:
 * Failed to set field default value ...}. This connector resolves column defaults itself (see the
 * DDL parser), so the JDBC-snapshot path never needs the driver-provided default.
 */
public class KafkaJsonJdbcConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonJdbcConnection.class);

    public KafkaJsonJdbcConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            String openingQuoteCharacter,
            String closingQuoteCharacter) {
        super(config, connectionFactory, openingQuoteCharacter, closingQuoteCharacter);
    }

    /**
     * Create definitions for each tables in the database, given the catalog name, schema pattern,
     * table filter, and column filter.
     *
     * @param tables the set of table definitions to be modified; may not be null
     * @param databaseCatalog the name of the catalog, which is typically the database name; may be
     *     null if all accessible databases are to be processed
     * @param schemaNamePattern the pattern used to match database schema names, which may be "" to
     *     match only those tables with no schema or null to process all accessible tables
     *     regardless of database schema name
     * @param tableFilter used to determine for which tables are to be processed; may be null if all
     *     accessible tables are to be processed
     * @param columnFilter used to determine which columns should be included as fields in its
     *     table's definition; may be null if all columns for all tables are to be included
     * @param removeTablesNotFoundInJdbc {@code true} if this method should remove from {@code
     *     tables} any definitions for tables that are not found in the database metadata, or {@code
     *     false} if such tables should be left untouched
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    @Override
    public void readSchema(
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.TableFilter tableFilter,
            ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        // Read the metadata for the table columns ...
        DatabaseMetaData metadata = connection().getMetaData();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> viewIds = new HashSet<>();
        final Set<TableId> tableIds = new HashSet<>();
        final Map<TableId, String> tableComments = new HashMap<>();

        int totalTables = 0;
        try (final ResultSet rs =
                metadata.getTables(
                        databaseCatalog, schemaNamePattern, null, supportedTableTypes())) {
            while (rs.next()) {
                final String catalogName = resolveCatalogName(rs.getString(1));
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final String tableType = rs.getString(4);
                TableId tableId;
                if (isTableType(tableType)) {
                    totalTables++;
                    tableId = new TableId(catalogName, schemaName, tableName);
                    if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                        tableIds.add(tableId);
                    }
                } else {
                    tableId = new TableId(catalogName, schemaName, tableName);
                    viewIds.add(tableId);
                }
                tableComments.put(tableId, rs.getString(5)); // "REMARKS" is table comment
            }
        }

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        if (totalTables == tableIds.size()
                || config().getBoolean(
                                RelationalDatabaseConnectorConfig
                                        .SNAPSHOT_FULL_COLUMN_SCAN_FORCE)) {
            columnsByTable =
                    getColumnsDetails(
                            databaseCatalog,
                            schemaNamePattern,
                            null,
                            tableFilter,
                            columnFilter,
                            metadata,
                            viewIds);
        } else {
            for (TableId includeTable : tableIds) {
                LOGGER.debug("Retrieving columns of table {}", includeTable);

                Map<TableId, List<Column>> cols =
                        getColumnsDetails(
                                databaseCatalog,
                                schemaNamePattern,
                                includeTable.table(),
                                tableFilter,
                                columnFilter,
                                metadata,
                                viewIds);
                columnsByTable.putAll(cols);
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames =
                    readPrimaryKeyOrUniqueIndexNames(metadata, tableEntry.getKey());

            // Then define the table — filling the ENUM/SET label lists first (the values are
            // not part of the column metadata ResultSet, they need their own query), so every
            // registered table is complete ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            columns = fillEnumAndSetLabels(columns, tableEntry.getKey());
            String defaultCharsetName = null; // JDBC does not expose character sets
            Table table =
                    tables.overwriteTable(
                            tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
            if (tableComments.containsKey(tableEntry.getKey())) {
                tables.overwriteTable(
                        table.edit().setComment(tableComments.get(tableEntry.getKey())).create());
            }
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    /**
     * Replicates {@link JdbcConnection#readTableColumn} without reading the {@code COLUMN_DEF}
     * default value: the MySQL driver returns it as a literal that Debezium cannot convert into a
     * typed schema default for binary (and some other) types, failing the schema read (see the
     * class javadoc).
     */
    @Override
    protected Optional<ColumnEditor> readTableColumn(
            ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter)
            throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null
                || columnFilter.matches(
                        tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            } catch (SQLException e) {
                // ignore, some drivers don't have this index
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));

            column.comment(columnMetadata.getString(12)); // "REMARKS" is column comment

            // Allow implementation to make column changes if required before being added to table
            column = overrideColumn(column);

            // Column default is intentionally not read (no getString(13) / defaultValueExpression).
            return Optional.of(column);
        }

        return Optional.empty();
    }

    /** Compensates the JDBC driver's column-metadata quirks right where the column is built. */
    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        String typeName = column.typeName().toUpperCase(Locale.ROOT);
        String base = typeName.split(" ")[0];
        // TINYINT (unsigned or not) maps to Types.SMALLINT
        if (column.jdbcType() == Types.TINYINT) {
            column.jdbcType(Types.SMALLINT);
        }
        // the shared mapping distinguishes TIMESTAMP from DATETIME by the type name
        if (column.jdbcType() == Types.TIMESTAMP && "TIMESTAMP".equals(typeName)) {
            column.jdbcType(Types.TIMESTAMP_WITH_TIMEZONE);
        }
        // approximate numerics (FLOAT/REAL/DOUBLE) carry no scale
        List<String> floatTypes = Arrays.asList("FLOAT", "REAL", "DOUBLE");
        if (floatTypes.contains(base)) {
            column.scale((Integer) null);
        }
        return column;
    }

    /**
     * Fills the ENUM/SET label lists of the columns.
     *
     * <p>MySQL proper reports the TYPE_NAME with the values ("enum('a','b','c')") — they are parsed
     * from the type name itself, in memory and without any query. Only TiDB's bare "ENUM"/"SET"
     * type names (no values in the metadata) fall back to a single {@code
     * information_schema.COLUMNS} query per table; tables without ENUM/SET columns never query at
     * all.
     */
    private List<Column> fillEnumAndSetLabels(List<Column> columns, TableId tableId)
            throws SQLException {
        // 1. the type names may already carry the values (MySQL proper) — parse them in memory
        List<Column> parsed = columns;
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            List<String> labels = MySqlEnumSetLabels.parse(column.typeName());
            if (labels != null) {
                if (parsed == columns) {
                    parsed = new ArrayList<>(columns);
                }
                parsed.set(i, column.edit().enumValues(labels).create());
            }
        }
        // 2. only the still-bare ENUM/SET columns (TiDB) need the information_schema query
        if (!needsLabelQuery(parsed)) {
            return parsed;
        }
        Map<String, String> columnTypes = new HashMap<>();
        String query =
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND DATA_TYPE IN ('enum','set')";
        try (PreparedStatement statement = connection().prepareStatement(query)) {
            statement.setString(1, tableId.catalog());
            statement.setString(2, tableId.table());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columnTypes.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        List<Column> filled = new ArrayList<>(parsed.size());
        for (Column column : parsed) {
            if (isEnumOrSetWithoutLabels(column)) {
                List<String> labels = MySqlEnumSetLabels.parse(columnTypes.get(column.name()));
                if (labels != null) {
                    filled.add(column.edit().enumValues(labels).create());
                    continue;
                }
            }
            filled.add(column);
        }
        return filled;
    }

    private static boolean needsLabelQuery(List<Column> columns) {
        for (Column column : columns) {
            if (isEnumOrSetWithoutLabels(column)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isEnumOrSetWithoutLabels(Column column) {
        List<String> labels = column.enumValues();
        if (labels != null && !labels.isEmpty()) {
            return false;
        }
        String typeName = column.typeName().toUpperCase(Locale.ROOT);
        return "ENUM".equals(typeName) || "SET".equals(typeName);
    }
}
