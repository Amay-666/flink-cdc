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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.ddl;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AddColumnEvent.ColumnWithPosition;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisDdlBuilder;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link DorisDdlBuilder}. */
public class DorisDdlBuilderTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_V2 = TableId.tableId("shop", "orders_v2");

    private DorisDdlBuilder builder(DorisDataSinkOptions options) {
        return new DorisDdlBuilder(options);
    }

    private DorisDdlBuilder defaultBuilder() {
        return builder(new DorisDataSinkOptions(new Configuration()));
    }

    @Test
    public void testCreateTableUniqueModel() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), "primary id")
                        .physicalColumn("name", DataTypes.VARCHAR(16), "the name")
                        .primaryKey("id")
                        .comment("order table")
                        .build();

        List<String> sqls = defaultBuilder().buildCreateTableSql(new CreateTableEvent(ORDERS, schema));

        assertThat(sqls)
                .containsExactly(
                        "CREATE TABLE IF NOT EXISTS `shop`.`orders` "
                                + "(`id` INT COMMENT 'primary id', `name` VARCHAR(16) COMMENT 'the name') "
                                + "COMMENT 'order table' "
                                + "UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS AUTO");
    }

    @Test
    public void testCreateTableDuplicateModelWhenNoPrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(16))
                        .build();

        List<String> sqls = defaultBuilder().buildCreateTableSql(new CreateTableEvent(ORDERS, schema));

        assertThat(sqls)
                .containsExactly(
                        "CREATE TABLE IF NOT EXISTS `shop`.`orders` "
                                + "(`id` INT, `name` VARCHAR(16)) "
                                + "DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS AUTO");
    }

    @Test
    public void testCreateTableTypeMapping() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("c_char", DataTypes.CHAR(8))
                        .physicalColumn("c_bool", DataTypes.BOOLEAN())
                        .physicalColumn("c_dec", DataTypes.DECIMAL(20, 4))
                        .physicalColumn("c_date", DataTypes.DATE())
                        // Precision is clamped to [0, 6] for Doris DATETIMEV2.
                        .physicalColumn("c_ts", DataTypes.TIMESTAMP(9))
                        .physicalColumn("c_ltz", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn("c_tz", DataTypes.TIMESTAMP_TZ())
                        .physicalColumn("c_arr", DataTypes.ARRAY(DataTypes.INT()))
                        .physicalColumn("c_map", DataTypes.MAP(DataTypes.VARCHAR(8), DataTypes.INT()))
                        .primaryKey("c_char")
                        .build();

        List<String> sqls = defaultBuilder().buildCreateTableSql(new CreateTableEvent(ORDERS, schema));

        assertThat(sqls.get(0))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS `shop`.`orders` "
                                + "(`c_char` CHAR(8), `c_bool` BOOLEAN, `c_dec` DECIMAL(20, 4), "
                                + "`c_date` DATE, `c_ts` DATETIMEV2(6), `c_ltz` DATETIMEV2(3), "
                                + "`c_tz` DATETIMEV2(6), `c_arr` STRING, `c_map` STRING) "
                                + "UNIQUE KEY(`c_char`) DISTRIBUTED BY HASH(`c_char`) BUCKETS AUTO");
    }

    @Test
    public void testAddColumnOneStatementPerColumn() {
        List<ColumnWithPosition> added =
                Arrays.asList(
                        new ColumnWithPosition(Column.physicalColumn("age", DataTypes.INT())),
                        new ColumnWithPosition(
                                Column.physicalColumn("addr", DataTypes.VARCHAR(64), "address")));

        List<String> sqls =
                defaultBuilder().buildAddColumnSql(new AddColumnEvent(ORDERS, added));

        assertThat(sqls)
                .containsExactly(
                        "ALTER TABLE `shop`.`orders` ADD COLUMN `age` INT",
                        "ALTER TABLE `shop`.`orders` ADD COLUMN `addr` VARCHAR(64) COMMENT 'address'");
    }

    @Test
    public void testDropColumn() {
        List<String> sqls =
                defaultBuilder()
                        .buildDropColumnSql(
                                new DropColumnEvent(ORDERS, Arrays.asList("age", "addr")));

        assertThat(sqls)
                .containsExactly(
                        "ALTER TABLE `shop`.`orders` DROP COLUMN `age`",
                        "ALTER TABLE `shop`.`orders` DROP COLUMN `addr`");
    }

    @Test
    public void testRenameColumn() {
        List<String> sqls =
                defaultBuilder()
                        .buildRenameColumnSql(
                                new RenameColumnEvent(
                                        ORDERS, Collections.singletonMap("old_name", "new_name")));

        assertThat(sqls)
                .containsExactly(
                        "ALTER TABLE `shop`.`orders` RENAME COLUMN `old_name` `new_name`");
    }

    @Test
    public void testAlterColumnType() {
        List<String> sqls =
                defaultBuilder()
                        .buildAlterColumnTypeSql(
                                new AlterColumnTypeEvent(
                                        ORDERS, Collections.singletonMap("price", DataTypes.DOUBLE())));

        assertThat(sqls).containsExactly("ALTER TABLE `shop`.`orders` MODIFY COLUMN `price` DOUBLE");
    }

    @Test
    public void testRenameTableUsesUnqualifiedNewName() {
        List<String> sqls =
                defaultBuilder()
                        .buildRenameTableSql(
                                new RenameTableEvent(
                                        ORDERS,
                                        ORDERS_V2,
                                        Schema.newBuilder()
                                                .physicalColumn("id", DataTypes.INT())
                                                .build()));

        assertThat(sqls)
                .containsExactly("ALTER TABLE `shop`.`orders` RENAME `orders_v2`");
    }

    @Test
    public void testDropTable() {
        List<String> sqls = defaultBuilder().buildDropTableSql(new DropTableEvent(ORDERS, null, null));

        assertThat(sqls).containsExactly("DROP TABLE IF EXISTS `shop`.`orders`");
    }

    @Test
    public void testTruncateTable() {
        List<String> sqls = defaultBuilder().buildTruncateTableSql(new TruncateTableEvent(ORDERS, null));

        assertThat(sqls).containsExactly("TRUNCATE TABLE `shop`.`orders`");
    }

    @Test
    public void testAlterTableComment() {
        List<String> sqls =
                defaultBuilder()
                        .buildAlterTableCommentSql(
                                new AlterTableCommentEvent(ORDERS, null, null, "new comment"));

        assertThat(sqls).containsExactly("ALTER TABLE `shop`.`orders` COMMENT 'new comment'");
    }

    @Test
    public void testAlterColumnComment() {
        List<String> sqls =
                defaultBuilder()
                        .buildAlterColumnCommentSql(
                                new AlterColumnCommentEvent(
                                        ORDERS, Collections.singletonMap("name", "display name")));

        assertThat(sqls)
                .containsExactly("ALTER TABLE `shop`.`orders` MODIFY COLUMN `name` COMMENT 'display name'");
    }

    @Test
    public void testTableNameMappingAppliesPrefixSuffix() {
        Configuration config = new Configuration();
        config.set(KafkaJsonDataSinkOptions.DATABASE_PREFIX, "dws_");
        config.set(KafkaJsonDataSinkOptions.TABLE_PREFIX, "ods_");
        DorisDdlBuilder builder =
                builder(new DorisDataSinkOptions(config));

        Schema schema =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();
        List<String> sqls = builder.buildCreateTableSql(new CreateTableEvent(ORDERS, schema));

        assertThat(sqls.get(0)).startsWith("CREATE TABLE IF NOT EXISTS `dws_shop`.`ods_orders`");
    }

    @Test
    public void testSingleQuoteEscapingInComments() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), "it's the id")
                        .primaryKey("id")
                        .comment("an \"order's\" comment")
                        .build();

        List<String> sqls = defaultBuilder().buildCreateTableSql(new CreateTableEvent(ORDERS, schema));

        assertThat(sqls.get(0))
                .contains("`id` INT COMMENT 'it''s the id'")
                .contains("COMMENT 'an \"order''s\" comment'");
    }
}
