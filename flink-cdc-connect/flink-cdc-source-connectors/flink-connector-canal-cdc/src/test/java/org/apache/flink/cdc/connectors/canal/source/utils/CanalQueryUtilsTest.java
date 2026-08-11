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

package org.apache.flink.cdc.connectors.canal.source.utils;

import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for the SQL generation of {@link CanalQueryUtils}. */
class CanalQueryUtilsTest {

    private static final TableId TABLE_ID = TableId.parse("test.users");

    private RowType singleKey() {
        return (RowType) ROW(FIELD("id", INT())).getLogicalType();
    }

    @Test
    void testQuoteTableId() {
        assertEquals("`test`.`users`", CanalQueryUtils.quote(TABLE_ID));
    }

    @Test
    void testQuoteColumnName() {
        assertEquals("`id`", CanalQueryUtils.quote("id"));
    }

    @Test
    void testBuildSplitScanQueryOnlySplit() {
        String sql = CanalQueryUtils.buildSplitScanQuery(TABLE_ID, singleKey(), true, true);
        assertEquals("SELECT * FROM `test`.`users`", sql);
    }

    @Test
    void testBuildSplitScanQueryFirstSplit() {
        String sql = CanalQueryUtils.buildSplitScanQuery(TABLE_ID, singleKey(), true, false);
        assertEquals(
                "SELECT * FROM `test`.`users` WHERE id <= ? AND NOT (id = ?)", sql);
    }

    @Test
    void testBuildSplitScanQueryLastSplit() {
        String sql = CanalQueryUtils.buildSplitScanQuery(TABLE_ID, singleKey(), false, true);
        assertEquals("SELECT * FROM `test`.`users` WHERE id >= ?", sql);
    }

    @Test
    void testBuildSplitScanQueryMiddleSplit() {
        String sql = CanalQueryUtils.buildSplitScanQuery(TABLE_ID, singleKey(), false, false);
        assertEquals(
                "SELECT * FROM `test`.`users` WHERE id >= ? AND NOT (id = ?) AND id <= ?", sql);
    }

    @Test
    void testBuildSplitScanQueryCompositeKey() {
        RowType compositeKey =
                (RowType) ROW(FIELD("a", INT()), FIELD("b", INT())).getLogicalType();
        String sql = CanalQueryUtils.buildSplitScanQuery(TABLE_ID, compositeKey, false, false);
        assertEquals(
                "SELECT * FROM `test`.`users` "
                        + "WHERE a >= ? AND b >= ? AND NOT (a = ? AND b = ?) AND a <= ? AND b <= ?",
                sql);
    }
}
