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

import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

/**
 * Parses the {@code sql} of one canal DDL message into a {@link KafkaJsonDdlParsedResult}.
 *
 * <p>Two implementations are selectable via {@code scan.ddl.parser}: {@link
 * KafkaJsonDruidDdlParser} (Alibaba Druid, the default) and {@link KafkaJsonDebeziumDdlParser} (Debezium's
 * MySQL ANTLR parser).
 */
public interface KafkaJsonDdlParser {

    /**
     * Parses the DDL statement into a schema change.
     *
     * @param database the database the message belongs to
     * @param tableId the table the message announces (from the message {@code database}/{@code
     *     table}); the canonical id of the affected table
     * @param currentTable the schema of the table before the DDL, or {@code null} if unknown
     * @param ddl the DDL statement
     * @return the parsed result, or {@code null} if the statement does not affect the table schema
     *     (e.g. index/truncate/use statements) or could not be parsed
     */
    @Nullable
    KafkaJsonDdlParsedResult parse(
            String database, TableId tableId, @Nullable Table currentTable, String ddl);
}
