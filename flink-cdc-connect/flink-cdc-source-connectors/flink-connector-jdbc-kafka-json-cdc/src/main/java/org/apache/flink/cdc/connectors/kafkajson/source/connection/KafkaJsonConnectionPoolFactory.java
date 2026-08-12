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

package org.apache.flink.cdc.connectors.kafkajson.source.connection;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

import com.zaxxer.hikari.HikariDataSource;

/** A connection pool factory to create pooled MySQL {@link HikariDataSource}. */
public class KafkaJsonConnectionPoolFactory extends JdbcConnectionPoolFactory {

    // sslMode=DISABLED is required: useSSL=false only maps to Connector/J's sslMode=PREFERRED,
    // which still attempts an encrypted handshake (and fails on a cert validity check when the
    // container clock drifts). Explicitly disabling SSL keeps the JDBC path deterministic, and
    // allowPublicKeyRetrieval is then needed for caching_sha2_password accounts (MySQL 8 default).
    public static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%s/?sslMode=DISABLED&allowPublicKeyRetrieval=true"
                    + "&useUnicode=true&characterEncoding=UTF-8&useCursorFetch=true"
                    + "&rewriteBatchedStatements=true";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        return String.format(JDBC_URL_PATTERN, hostName, port);
    }
}
