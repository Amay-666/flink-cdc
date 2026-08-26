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

package org.apache.flink.cdc.connectors.kafkajson.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonColumnMeta;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonQueryUtils.quote;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class KafkaJsonSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSchemaUtils.class);

    /** Opens a JDBC connection to the MySQL server described by the given source config. */
    public static JdbcConnection openJdbcConnection(KafkaJsonSourceConfig sourceConfig) {
        return new KafkaJsonDialect(sourceConfig).openJdbcConnection(sourceConfig);
    }

    public static List<String> listDatabases(KafkaJsonSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            KafkaJsonSourceConfig sourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbc);

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list tables: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(KafkaJsonSourceConfig sourceConfig, TableId tableId) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return getTableSchema(jdbc, sourceConfig, tableId);
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Error to get table schema for " + tableId + ": " + e.getMessage(), e);
        }
    }

    /** Reads the schema of the given table using an already-opened connection. */
    public static Schema getTableSchema(
            JdbcConnection jdbc, KafkaJsonSourceConfig sourceConfig, TableId tableId) {
        TableChanges.TableChange tableChange =
                new KafkaJsonDialect(sourceConfig).queryTableSchema(jdbc, toDbzTableId(tableId));
        return toSchema(tableChange.getTable());
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        jdbc.query(
                "SHOW DATABASES WHERE `database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String dbName) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(dbName, rs.getString(1)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(KafkaJsonSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(),
                KafkaJsonColumnMeta.fromColumn(column).toCdcDataType(column.isOptional()),
                column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static TableId toCommonTableId(io.debezium.relational.TableId tableId) {
        return TableId.tableId(tableId.catalog(), tableId.table());
    }

    private KafkaJsonSchemaUtils() {}
}
