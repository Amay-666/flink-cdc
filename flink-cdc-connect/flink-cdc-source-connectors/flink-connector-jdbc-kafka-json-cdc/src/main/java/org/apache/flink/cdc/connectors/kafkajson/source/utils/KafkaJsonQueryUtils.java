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

package org.apache.flink.cdc.connectors.kafkajson.source.utils;

import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/** Query-related utilities for the Canal (MySQL) source. */
public class KafkaJsonQueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonQueryUtils.class);

    private KafkaJsonQueryUtils() {}

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quote(column.name()), quote(column.name()), quote(tableId));
        return jdbc.queryAndMap(
                minMaxQuery,
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        minMaxQuery));
                    }
                    return rowToArray(rs, 2);
                });
    }

    public static Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // Use the statistics in information_schema, which is much more efficient than COUNT(*)
        // for large tables.
        final String query =
                String.format(
                        "SELECT TABLE_ROWS FROM information_schema.TABLES "
                                + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        tableId.catalog(), tableId.table());
        return jdbc.queryAndMap(
                query,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    long approximateRowCnt = rs.getLong(1);
                    LOG.info("queryApproximateRowCnt: {} => {}", query, approximateRowCnt);
                    return approximateRowCnt;
                });
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        final String query =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(column.name()), quote(tableId), quote(column.name()));
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    LOG.info("{} => {}", query, rs.getObject(1));
                    return rs.getObject(1);
                });
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumn.name());
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quotedColumn,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        quotedColumn,
                        chunkSize);
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, includedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    /** Builds the SQL to scan the rows of a snapshot split. */
    public static String buildSplitScanQuery(
            TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        final String condition = buildScanCondition(pkRowType, isFirstSplit, isLastSplit);
        final StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(quote(tableId));
        if (condition != null) {
            sql.append(" WHERE ").append(condition);
        }
        return sql.toString();
    }

    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('`');
    }

    private static String buildScanCondition(
            RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        if (isFirstSplit && isLastSplit) {
            return null;
        }
        if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ");
            sql.append(" AND NOT (");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ");
            sql.append(")");
            return sql.toString();
        }
        if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ");
            return sql.toString();
        }
        // middle split: [start, end]
        final StringBuilder sql = new StringBuilder();
        addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ");
        sql.append(" AND NOT (");
        addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ");
        sql.append(")");
        sql.append(" AND ");
        addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ");
        return sql.toString();
    }

    private static void addPrimaryKeyColumnsToCondition(
            RowType pkRowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate).append("?");
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }
}
