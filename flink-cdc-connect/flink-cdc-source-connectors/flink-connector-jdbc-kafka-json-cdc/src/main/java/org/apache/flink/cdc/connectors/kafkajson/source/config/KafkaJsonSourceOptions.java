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

package org.apache.flink.cdc.connectors.kafkajson.source.config;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Locale;

/** Configurations for the Canal source. */
public class KafkaJsonSourceOptions extends JdbcSourceOptions {

    /** The port of the MySQL server, used for the JDBC snapshot phase. */
    public static final ConfigOption<Integer> CANAL_MYSQL_PORT =
            ConfigOptions.key("port").intType().defaultValue(3306).withDescription(
                    "Integer port number of the MySQL database server.");

    /** The session time zone in the MySQL database server, e.g. "UTC". */
    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in the MySQL database server, e.g. \"UTC\". "
                                    + "When not set, the Flink session time zone is used.");

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
     * the SQL values are kebab-case (e.g. {@code at-least-once}) which Flink's enum conversion does
     * not accept; the {@link MessageFormat} enum is resolved in the table factory.
     */
    public static final ConfigOption<String> MESSAGE_FORMAT =
            ConfigOptions.key("scan.message.format")
                    .stringType()
                    .defaultValue(MessageFormat.CANAL.toString().toLowerCase(Locale.ROOT))
                    .withDescription(
                            "The message format of the Kafka messages, either 'canal' (flatMessage JSON, default) or 'debezium'.");

    /**
     * The database type of the source, used to select the JDBC/dialect layer. Declared as a string
     * for the same kebab-case reason as {@link #MESSAGE_FORMAT}; the {@link DatabaseType} enum is
     * resolved in the factory. Only 'mysql' and 'tidb' are implemented in this version; 'tidb'
     * reuses the MySQL-compatible JDBC/dialect path (TiDB speaks the MySQL wire protocol).
     */
    public static final ConfigOption<String> DATABASE_TYPE =
            ConfigOptions.key("scan.database.type")
                    .stringType()
                    .defaultValue(DatabaseType.MYSQL.toString().toLowerCase(Locale.ROOT))
                    .withDescription(
                            "The database type of the source: 'mysql' (default), 'postgres', or 'tidb'. "
                                    + "Only 'mysql' and 'tidb' are implemented in this version; other values fail at startup.");

    /** The timestamp field used as the offset event time. */
    public static final ConfigOption<String> EVENT_TIME =
            ConfigOptions.key("scan.message.event-time")
                    .stringType()
                    .defaultValue(EventTime.ES.toString().toLowerCase(Locale.ROOT))
                    .withDescription(
                            "The timestamp used as the offset event time for canal messages: 'es' (binlog execution time, default) or 'ts' (canal send time).");

    /** How to handle the boundary when a stream message timestamp equals the snapshot high watermark. */
    public static final ConfigOption<String> BOUNDARY_MODE =
            ConfigOptions.key("scan.boundary.mode")
                    .stringType()
                    .defaultValue(
                            BoundaryMode.EXACTLY_ONCE
                                    .toString()
                                    .toLowerCase(Locale.ROOT)
                                    .replace('_', '-'))
                    .withDescription(
                            "The boundary handling mode: 'exactly-once' drops stream messages whose timestamp "
                                    + "equals the snapshot high watermark (strict snapshot-first, default); "
                                    + "'at-least-once' keeps them at the cost of possible duplicates.");

    /** Kafka consumer startup mode used for stream-only scenarios. */
    public static final ConfigOption<String> KAFKA_STARTUP_MODE =
            ConfigOptions.key("scan.kafka.startup.mode")
                    .stringType()
                    .defaultValue(KafkaStartupMode.EARLIEST.toString().toLowerCase(Locale.ROOT))
                    .withDescription(
                            "Kafka consumer startup mode for stream-only scenarios: 'earliest' (default), 'latest' or 'timestamp'.");

    /** The DDL parser implementation. */
    public static final ConfigOption<String> CANAL_DDL_PARSER =
            ConfigOptions.key("scan.ddl.parser")
                    .stringType()
                    .defaultValue(DdlParser.DRUID.toString().toLowerCase(Locale.ROOT))
                    .withDescription(
                            "The DDL parser implementation: 'druid' (Alibaba Druid, default) or 'debezium' (Debezium ANTLR).");

    /** Prefix of the arbitrary Kafka consumer properties. */
    public static final String KAFKA_PROPERTIES_PREFIX = "scan.kafka.properties.";

    /** Message format of the Kafka messages produced by external tools. */
    public enum MessageFormat {
        CANAL,
        DEBEZIUM
    }

    /** Database type of the source, selected by {@code scan.database.type}. */
    public enum DatabaseType {
        MYSQL,
        POSTGRES,
        TIDB
    }

    /** Timestamp field of a canal message used as the offset event time. */
    public enum EventTime {
        ES,
        TS,
        TIDB_TSO
    }

    /** Boundary handling mode of the full->incremental switch. */
    public enum BoundaryMode {
        EXACTLY_ONCE,
        AT_LEAST_ONCE
    }

    /** Kafka consumer startup mode. */
    public enum KafkaStartupMode {
        EARLIEST,
        LATEST,
        TIMESTAMP
    }

    /** DDL parser implementation. */
    public enum DdlParser {
        DRUID,
        DEBEZIUM
    }
}
