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

package org.apache.flink.cdc.connectors.kafkajson.source.utils;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Queries the TiDB cluster's current TSO as the snapshot low/high watermark.
 *
 * <p>This is the TiDB alternative to {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.kafka.KafkaJsonKafkaOffsetUtils#queryCurrentOffset}.
 * The Kafka-sampled watermark is the maximum {@code es}/{@code ts} of the newest published message,
 * which trails the database "now" by the publish lag of the change-capture tool: changes committed
 * during a snapshot split's JDBC read but not yet published when the watermark was sampled fall
 * outside the split's {@code (low, high]} backfill window and are re-emitted by the stream phase
 * (duplicates), and an empty topic yields no watermark at all ({@link
 * KafkaJsonOffset#INITIAL_OFFSET}), which disables the backfill and forces the stream onto the
 * Kafka startup mode. Querying the database instead of Kafka sidesteps both: a TSO is the
 * authoritative, monotonically increasing commit-clock position, so its physical millisecond value
 * is an upper bound on the {@code es} (commit time) of every change already visible to the JDBC
 * snapshot read, and it exists even while the topic is still empty.
 *
 * <p>TSO is a commit-clock value, so it is only a valid boundary for {@code
 * scan.message.event-time=es} (the default); with {@code ts} (producer send time) the boundary must
 * stay on the Kafka-sampled value (see {@code KafkaJsonDialect#displayCurrentOffset}).
 *
 * <p>{@code TIDB_CURRENT_TSO()} returns {@code 0} outside a transaction, so the query runs inside a
 * read-only transaction whose start timestamp is the freshly allocated TSO, and is immediately
 * rolled back. A TSO is the physical time (millis since epoch) in the upper 46 bits plus an 18-bit
 * logical counter; the event time is therefore {@code tso &gt;&gt; 18}.
 *
 * <p>This is a best-effort enhancement: any failure returns {@code null} and the caller falls back
 * to the Kafka-sampled boundary rather than failing the snapshot.
 */
public class KafkaJsonTidbOffsetUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonTidbOffsetUtils.class);

    /** SQL to fetch the current TSO; must run inside a transaction to return a non-zero value. */
    private static final String SELECT_CURRENT_TSO = "SELECT TIDB_CURRENT_TSO()";

    // Mirrors KafkaJsonConnectionPoolFactory's URL (sslMode=DISABLED keeps the handshake
    // deterministic for containers, allowPublicKeyRetrieval is needed by caching_sha2_password
    // accounts) without the cursor-fetch/batch flags that only matter for the snapshot reads.
    private static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%s/?sslMode=DISABLED&allowPublicKeyRetrieval=true"
                    + "&useUnicode=true&characterEncoding=UTF-8&connectTimeout=%d";

    private KafkaJsonTidbOffsetUtils() {}

    /**
     * Returns the current TiDB commit-clock position as a {@link KafkaJsonOffset} stamped on the
     * sentinel partition/offset, or {@code null} when the TSO cannot be queried.
     */
    @Nullable
    public static KafkaJsonOffset queryCurrentOffset(KafkaJsonSourceConfig sourceConfig) {
        try (Connection connection = openConnection(sourceConfig)) {
            // A TSO is only allocated inside a transaction; abandon the read-only transaction after
            // the query so it has no side effect.
            connection.setAutoCommit(false);
            long tso;
            try {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("BEGIN");
                    ResultSet resultSet = statement.executeQuery(SELECT_CURRENT_TSO);
                    if (!resultSet.next()) {
                        LOG.warn("{} returned no row", SELECT_CURRENT_TSO);
                        return null;
                    }
                    tso = resultSet.getLong(1);
                }
            } finally {
                rollbackQuietly(connection);
            }
            if (tso <= 0) {
                LOG.warn(
                        "{} returned {} (a TSO is only returned inside a transaction); refusing to"
                                + " use it as the snapshot boundary",
                        SELECT_CURRENT_TSO,
                        tso);
                return null;
            }
            long eventTime = tsoToEventTime(tso);
            LOG.debug("TiDB current TSO {} -> event time {} ms", tso, eventTime);
            return new KafkaJsonOffset(eventTime, Integer.MAX_VALUE, Long.MAX_VALUE);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to query the TiDB current TSO; falling back to the Kafka-sampled boundary",
                    e);
            return null;
        }
    }

    /**
     * Converts a TiDB TSO to Unix epoch millis. The physical time occupies the upper 46 bits; the
     * lower 18 bits are the logical counter.
     */
    public static long tsoToEventTime(long tso) {
        return tso >> 18;
    }

    private static Connection openConnection(KafkaJsonSourceConfig sourceConfig)
            throws SQLException {
        Duration connectTimeout = sourceConfig.getConnectTimeout();
        String url =
                String.format(
                        JDBC_URL_PATTERN,
                        sourceConfig.getHostname(),
                        sourceConfig.getPort(),
                        connectTimeout == null ? 30_000L : connectTimeout.toMillis());
        return DriverManager.getConnection(
                url, sourceConfig.getUsername(), sourceConfig.getPassword());
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.warn("Failed to roll back the TSO transaction", e);
        }
    }
}
