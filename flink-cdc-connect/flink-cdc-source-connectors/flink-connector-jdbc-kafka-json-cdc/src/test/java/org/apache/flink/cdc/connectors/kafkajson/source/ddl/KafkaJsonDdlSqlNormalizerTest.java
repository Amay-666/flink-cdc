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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link KafkaJsonDdlSqlNormalizer}. */
class KafkaJsonDdlSqlNormalizerTest {

    @Test
    void stripsAddColumnIfNotExists() {
        assertEquals(
                "ALTER TABLE `test`.`users` ADD COLUMN `age` int",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `test`.`users` ADD COLUMN IF NOT EXISTS `age` int"));
    }

    @Test
    void stripsDropColumnIfExists() {
        assertEquals(
                "ALTER TABLE `test`.`users` DROP COLUMN `age`",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `test`.`users` DROP COLUMN IF EXISTS `age`"));
    }

    @Test
    void stripsIndexClauses() {
        assertEquals(
                "ALTER TABLE `test`.`users` ADD INDEX idx_a (`age`)",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `test`.`users` ADD INDEX IF NOT EXISTS idx_a (`age`)"));
        assertEquals(
                "ALTER TABLE `test`.`users` DROP INDEX idx_a",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `test`.`users` DROP INDEX IF EXISTS idx_a"));
    }

    @Test
    void isCaseInsensitive() {
        assertEquals(
                "alter table `t` add column `c` int",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "alter table `t` add column if not exists `c` int"));
    }

    @Test
    void toleratesIrregularWhitespace() {
        // The clause may span newlines/tabs; surrounding whitespace elsewhere is preserved, the
        // gap left by the clause collapses to a single space.
        assertEquals(
                "ALTER TABLE `t` ADD   COLUMN `c` int",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` ADD   COLUMN IF\nNOT\tEXISTS `c` int"));
    }

    @Test
    void keepsClauseInsideStringLiteral() {
        assertEquals(
                "ALTER TABLE `t` ADD COLUMN `c` varchar(20) COMMENT 'IF NOT EXISTS note'",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` ADD COLUMN `c` varchar(20) COMMENT 'IF NOT EXISTS note'"));
    }

    @Test
    void keepsClauseInsideBlockComment() {
        assertEquals(
                "ALTER TABLE `t` ADD COLUMN /* IF NOT EXISTS */ `c` int",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` ADD COLUMN /* IF NOT EXISTS */ `c` int"));
    }

    @Test
    void keepsClauseInsideQuotedIdentifier() {
        assertEquals(
                "ALTER TABLE `t` DROP COLUMN `IF EXISTS`",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` DROP COLUMN `IF EXISTS`"));
    }

    @Test
    void bridgesWithSingleSpaceWhenTokensAreTouching() {
        // "EXISTS`c`" (no whitespace before the backtick) must not fuse into "EXISTS`c`" being
        // dropped; the removal leaves a single space so the statement stays parseable.
        assertEquals(
                "ALTER TABLE `t` ADD COLUMN `c` int",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS`c` int"));
    }

    @Test
    void leavesSqlWithoutIfExistsUntouched() {
        String ddl = "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(255) NOT NULL";
        assertEquals(ddl, KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(ddl));
    }

    @Test
    void doesNotStripNotWithoutFollowingExists() {
        // "IF NOT NULL" is not an idempotence clause: only IF [NOT] EXISTS is stripped.
        assertEquals(
                "ALTER TABLE `t` MODIFY COLUMN `c` int NOT NULL",
                KafkaJsonDdlSqlNormalizer.stripIfExistsClauses(
                        "ALTER TABLE `t` MODIFY COLUMN `c` int NOT NULL"));
    }
}
