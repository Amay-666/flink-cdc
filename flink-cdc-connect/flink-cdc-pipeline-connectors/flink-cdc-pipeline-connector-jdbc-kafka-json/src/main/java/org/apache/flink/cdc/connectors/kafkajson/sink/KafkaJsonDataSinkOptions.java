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

package org.apache.flink.cdc.connectors.kafkajson.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Options shared by the kafka-json sink dialects.
 *
 * <p>Only the options every target sink shares live here: the table/database prefixes and suffixes
 * that map a source {@code TableId} onto the target's database/table before any DDL or data request
 * is issued, so a pipeline can map {@code db.tbl} to another database/table in the target without
 * changing the source schema. Target-specific options belong to each engine's own options class —
 * e.g. the Doris {@code fenodes}/{@code username} live in {@code
 * org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions}, which extends
 * this class.
 */
public class KafkaJsonDataSinkOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> DATABASE_PREFIX =
            ConfigOptions.key("sink.database-prefix").stringType().defaultValue("");

    public static final ConfigOption<String> DATABASE_SUFFIX =
            ConfigOptions.key("sink.database-suffix").stringType().defaultValue("");

    public static final ConfigOption<String> TABLE_PREFIX =
            ConfigOptions.key("sink.table-prefix").stringType().defaultValue("");

    public static final ConfigOption<String> TABLE_SUFFIX =
            ConfigOptions.key("sink.table-suffix").stringType().defaultValue("");

    private final Configuration config;

    public KafkaJsonDataSinkOptions(Configuration config) {
        this.config = config;
    }

    public Configuration getConfig() {
        return config;
    }

    public String getDatabasePrefix() {
        return config.get(DATABASE_PREFIX);
    }

    public String getDatabaseSuffix() {
        return config.get(DATABASE_SUFFIX);
    }

    public String getTablePrefix() {
        return config.get(TABLE_PREFIX);
    }

    public String getTableSuffix() {
        return config.get(TABLE_SUFFIX);
    }

    /** Maps the source database name onto the target database name via the configured prefix/suffix. */
    public String mapDatabase(TableId tableId) {
        return getDatabasePrefix() + tableId.getSchemaName() + getDatabaseSuffix();
    }

    /** Maps the source table name onto the target table name via the configured prefix/suffix. */
    public String mapTable(TableId tableId) {
        return getTablePrefix() + tableId.getTableName() + getTableSuffix();
    }
}
