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

/**
 * The type of a schema change parsed from one canal DDL message.
 *
 * <p>Extends the base {@code CREATE}/{@code ALTER}/{@code DROP} triple of Debezium's {@code
 * TableChanges.TableChangeType} with the rename and truncate changes that the released type cannot
 * express. The canal connector uses it to announce a table/column rename or truncate to a downstream
 * that builds its own event handling (the released flink-cdc runtime has no {@code RENAME_TABLE}
 * event, so a table rename would otherwise be flattened into a {@code DROP}+{@code CREATE} pair;
 * likewise, a truncate would be indistinguishable from a {@code DROP}+{@code CREATE} pair).
 *
 * <p>For column-level changes, the specific change details are recorded in a {@link ColumnChangeInfo}
 * list so that downstream handlers can react differently to each kind of schema evolution (e.g.,
 * type change vs. comment change). The Debezium ANTLR parser ({@link
 * KafkaJsonDebeziumDdlParser}) cannot distinguish these subtypes and always reports a plain {@code
 * ALTER}; the Druid parser ({@link KafkaJsonDruidDdlParser}) provides the full detail by comparing
 * the old and new column definitions.
 */
public enum KafkaJsonTableChangeType {
    CREATE,
    ALTER,
    DROP,
    RENAME_TABLE,
    RENAME_COLUMN,
    TRUNCATE,

    // --- Table-level ALTER operations ---
    ADD_COLUMN,
    DROP_COLUMN,

    // --- Column-level ALTER operations (with specific change type) ---
    ALTER_COLUMN_TYPE,
    ALTER_COLUMN_COMMENT,
    ALTER_COLUMN_POSITION
}
