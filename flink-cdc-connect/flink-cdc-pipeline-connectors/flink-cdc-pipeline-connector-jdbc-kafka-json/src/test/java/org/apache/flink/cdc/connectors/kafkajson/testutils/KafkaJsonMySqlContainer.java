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

package org.apache.flink.cdc.connectors.kafkajson.testutils;

import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;

/**
 * Test {@link MySqlContainer} that forces {@code sslMode=DISABLED} on its JDBC URL.
 *
 * <p>The released {@link MySqlContainer} (and testcontainers) only appends {@code useSSL=false},
 * which Connector/J 8.x maps to {@code sslMode=PREFERRED}: the driver still attempts an encrypted
 * handshake and only falls back when the server lacks SSL. Under WSL2 the container and host clocks
 * can drift by seconds, making MySQL's auto-generated certificate "not yet valid" and killing the
 * connection with {@code CertificateNotYetValidException}. Disabling SSL removes certificate
 * validity from the equation entirely.
 */
public class KafkaJsonMySqlContainer extends MySqlContainer {

    private static final String SSL_MODE_PARAM = "sslMode=DISABLED";
    private static final String PUBLIC_KEY_RETRIEVAL_PARAM = "allowPublicKeyRetrieval=true";

    public KafkaJsonMySqlContainer(MySqlVersion version) {
        super(version);
    }

    /**
     * Pins the container's MySQL port (3306) to a fixed host port, so the database is reachable
     * from Windows at a stable {@code localhost:&lt;hostPort&gt;}.
     *
     * <p>testcontainers 1.18.3 only exposes the fluent {@code withFixedExposedPort} on {@code
     * FixedHostPortGenericContainer}; on {@link org.testcontainers.containers.GenericContainer} the
     * equivalent {@code addFixedExposedPort} is protected, so it is surfaced through this subclass.
     */
    public KafkaJsonMySqlContainer withFixedExposedPort(int hostPort, int containerPort) {
        addFixedExposedPort(hostPort, containerPort);
        return this;
    }

    @Override
    public String getJdbcUrl(String databaseName) {
        // The released MySqlContainer's getJdbcUrl() carries no query parameters (its
        // useSSL/allowPublicKeyRetrieval handling lives in constructUrlForConnection, which
        // getJdbcUrl() does not use). With SSL disabled, MySQL 8.0's caching_sha2_password
        // requires allowPublicKeyRetrieval=true for the password exchange.
        String url = super.getJdbcUrl(databaseName);
        String separator = url.contains("?") ? "&" : "?";
        return url + separator + SSL_MODE_PARAM + "&" + PUBLIC_KEY_RETRIEVAL_PARAM;
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(getDatabaseName());
    }
}
