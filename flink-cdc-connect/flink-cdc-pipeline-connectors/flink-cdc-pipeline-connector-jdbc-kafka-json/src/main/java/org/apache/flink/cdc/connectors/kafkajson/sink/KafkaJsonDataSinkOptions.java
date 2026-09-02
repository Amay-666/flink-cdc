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
import java.util.function.Function;

/**
 * Options shared by the kafka-json sink dialects.
 *
 * <p>Only the options every target sink shares live here: the mapping of a source {@code TableId}
 * onto the target's database/table name, applied before any DDL or data request is issued so a
 * pipeline can map {@code db.tbl} to another database/table in the target without changing the
 * source schema. The mapping is a {@link Function} — install one via {@link
 * #withDatabaseMapping}/{@link #withTableMapping} — and defaults to the {@code
 * sink.database-prefix}/{@code -suffix} and {@code sink.table-prefix}/{@code -suffix} config
 * options. Target-specific options belong to each engine's own options class — e.g. the Doris
 * {@code fenodes}/{@code username} live in {@code
 * org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions}, which extends
 * this class.
 */
public class KafkaJsonDataSinkOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A serializable {@code TableId → name} mapping. The {@code Serializable} upper bound matters:
     * options objects are Java-serialized into the job graph, so a plain {@code Function} lambda
     * (whose target interface does not extend {@code Serializable}) would fail at submission time.
     */
    @FunctionalInterface
    public interface TableIdMapping extends Function<TableId, String>, Serializable {
        @Override
        String apply(TableId tableId);
    }

    public static final ConfigOption<String> DATABASE_PREFIX =
            ConfigOptions.key("sink.database-prefix").stringType().defaultValue("");

    public static final ConfigOption<String> DATABASE_SUFFIX =
            ConfigOptions.key("sink.database-suffix").stringType().defaultValue("");

    public static final ConfigOption<String> TABLE_PREFIX =
            ConfigOptions.key("sink.table-prefix").stringType().defaultValue("");

    public static final ConfigOption<String> TABLE_SUFFIX =
            ConfigOptions.key("sink.table-suffix").stringType().defaultValue("");

    private final Configuration config;

    /** Custom mappings; {@code null} means "use the configured prefix/suffix". */
    private volatile TableIdMapping databaseMapping;

    private volatile TableIdMapping tableMapping;

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

    /**
     * Replaces the prefix/suffix database mapping with an arbitrary function, e.g. {@code
     * options.withDatabaseMapping(t -> "ods_" + t.getSchemaName())}. Mutates and returns this
     * instance; call it before the options are serialized into the job graph.
     */
    public KafkaJsonDataSinkOptions withDatabaseMapping(TableIdMapping mapping) {
        this.databaseMapping = mapping;
        return this;
    }

    /**
     * Replaces the prefix/suffix table mapping with an arbitrary function; see {@link
     * #withDatabaseMapping}.
     */
    public KafkaJsonDataSinkOptions withTableMapping(TableIdMapping mapping) {
        this.tableMapping = mapping;
        return this;
    }

    /** Maps the source database name onto the target database name. */
    public String mapDatabase(TableId tableId) {
        TableIdMapping mapping = databaseMapping;
        return mapping != null
                ? mapping.apply(tableId)
                : getDatabasePrefix() + tableId.getSchemaName() + getDatabaseSuffix();
    }

    /** Maps the source table name onto the target table name. */
    public String mapTable(TableId tableId) {
        TableIdMapping mapping = tableMapping;
        return mapping != null
                ? mapping.apply(tableId)
                : getTablePrefix() + tableId.getTableName() + getTableSuffix();
    }
}
