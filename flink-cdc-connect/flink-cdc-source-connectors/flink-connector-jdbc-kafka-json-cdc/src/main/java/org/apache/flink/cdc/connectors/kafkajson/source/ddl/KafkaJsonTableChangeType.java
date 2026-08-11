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
 * TableChanges.TableChangeType} with the rename changes that the released type cannot express. The
 * canal connector uses it to announce a table/column rename to a downstream that builds its own
 * event handling (the released flink-cdc runtime has no {@code RENAME_TABLE} event, so a table
 * rename would otherwise be flattened into a {@code DROP}+{@code CREATE} pair).
 */
public enum KafkaJsonTableChangeType {
    CREATE,
    ALTER,
    DROP,
    RENAME_TABLE,
    RENAME_COLUMN
}
