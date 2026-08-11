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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link KafkaJsonDataSource}. */
@PublicEvolving
public class KafkaJsonDataSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the MySQL database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Integer port number of the MySQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the MySQL database to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the MySQL tables to monitor. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the MySQL database server before timing out.");

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            ConfigOptions.key("connection.pool.size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The connection pool size.");

    public static final ConfigOption<Integer> CONNECT_MAX_RETRIES =
            ConfigOptions.key("connect.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times that the connector should retry to build MySQL database server connection.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for Canal CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    /** Kafka bootstrap servers that external tools (canal) write the messages to. */
    public static final ConfigOption<String> KAFKA_BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.kafka.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kafka bootstrap servers of the message broker that the external tools (canal) write the incremental changes to.");

    /** Kafka consumer group id. Offsets are not committed by default. */
    public static final ConfigOption<String> KAFKA_GROUP_ID =
            ConfigOptions.key("properties.kafka.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kafka consumer group id. If not specified, a random group id is used. "
                                    + "Offsets are not committed to Kafka by default.");

    /** Comma-separated list of Kafka topics that carry the incremental messages. */
    public static final ConfigOption<String> SCAN_KAFKA_TOPICS =
            ConfigOptions.key("scan.kafka.topics")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of Kafka topics (or a regex) that carry the incremental data and DDL messages.");

    /**
     * The message format of the Kafka messages. Declared as a string (not {@code enumType}) because
     * the values are kebab-case (e.g. {@code at-least-once}) which Flink's enum conversion does not
     * accept; the {@link KafkaJsonSourceOptions.MessageFormat} enum is resolved in the factory.
     */
    public static final ConfigOption<String> MESSAGE_FORMAT =
            ConfigOptions.key("scan.message.format")
                    .stringType()
                    .defaultValue("canal")
                    .withDescription(
                            "The message format of the Kafka messages, either 'canal' (flatMessage JSON, default) or 'debezium'.");

    /**
     * The database type of the source, used to select the JDBC/dialect layer. Declared as a string
     * for the same kebab-case reason as {@link #MESSAGE_FORMAT}; the {@link
     * KafkaJsonSourceOptions.DatabaseType} enum is resolved in the factory. Only 'mysql' is
     * implemented in this version.
     */
    public static final ConfigOption<String> DATABASE_TYPE =
            ConfigOptions.key("scan.database.type")
                    .stringType()
                    .defaultValue("mysql")
                    .withDescription(
                            "The database type of the source: 'mysql' (default) or 'postgres'. "
                                    + "Only 'mysql' is implemented in this version; other values fail at startup.");

    /** The timestamp field used as the offset event time. */
    public static final ConfigOption<String> EVENT_TIME =
            ConfigOptions.key("scan.message.event-time")
                    .stringType()
                    .defaultValue("es")
                    .withDescription(
                            "The timestamp used as the offset event time for canal messages: 'es' (binlog execution time, default) or 'ts' (canal send time).");

    /** How to handle the boundary when a stream message timestamp equals the snapshot high watermark. */
    public static final ConfigOption<String> BOUNDARY_MODE =
            ConfigOptions.key("scan.boundary.mode")
                    .stringType()
                    .defaultValue("exactly-once")
                    .withDescription(
                            "The boundary handling mode: 'exactly-once' drops stream messages whose timestamp "
                                    + "equals the snapshot high watermark (strict snapshot-first, default); "
                                    + "'at-least-once' keeps them at the cost of possible duplicates.");

    /** Kafka consumer startup mode used for stream-only scenarios. */
    public static final ConfigOption<String> KAFKA_STARTUP_MODE =
            ConfigOptions.key("scan.kafka.startup.mode")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription(
                            "Kafka consumer startup mode for stream-only scenarios: 'earliest' (default), 'latest' or 'timestamp'.");

    /** The DDL parser implementation. */
    public static final ConfigOption<String> CANAL_DDL_PARSER =
            ConfigOptions.key("scan.ddl.parser")
                    .stringType()
                    .defaultValue("druid")
                    .withDescription(
                            "The DDL parser implementation: 'druid' (Alibaba Druid, default) or 'debezium' (Debezium ANTLR).");

    // ----------------------------------------------------------------------------
    // experimental options, won't add them to documentation
    // ----------------------------------------------------------------------------
    @Experimental
    public static final ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query MySQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distributed or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query MySQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table."
                                    + "By default, the chunk key is the first column of the primary key."
                                    + "eg. db1.user_table_[0-9]+:col1;db[1-2].[app|web]_order_\\.*:col2;");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase. This feature depends on "
                                    + "FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be "
                                    + "greater than or equal to 1.14 when enabling this feature.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to scan the newly added tables or not, by default is false. This option is only useful when we start the job from a savepoint/checkpoint.");

    @Experimental
    public static final ConfigOption<Boolean> SCHEMA_CHANGE_ENABLED =
            ConfigOptions.key("schema-change.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether send schema change events, by default is true. If set to false, the schema changes will not be sent.");

    private KafkaJsonDataSourceOptions() {}
}
