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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test for the committed {@code e2e/env.yaml.template}: every key the E2E tests read must
 * be present with the right shape, so a renamed key fails here immediately instead of at the next
 * E2E run. The provisioned {@code e2e/env.yaml} itself is local and git-ignored.
 */
class E2eConfigTest {

    @Test
    void templateCarriesEveryKeyTheE2eReads() {
        E2eConfig config = E2eConfig.loadFrom(E2eConfig.TEMPLATE_RESOURCE);

        for (String key :
                Arrays.asList(
                        "mysql.host",
                        "mysql.user",
                        "mysql.password",
                        "mysql.database",
                        "doris.feHttp",
                        "doris.queryHost",
                        "doris.user",
                        "doris.password",
                        "doris.database",
                        "kafka.servers",
                        "kafka.group",
                        "kafka.topic")) {
            assertNotNull(config.getString(key), "missing string key '" + key + "'");
        }

        for (String key :
                Arrays.asList(
                        "mysql.port",
                        "doris.queryPort",
                        "timing.pollIntervalMs",
                        "timing.pollTimeoutMs")) {
            assertTrue(config.getInt(key) > 0, "missing or non-positive int key '" + key + "'");
        }
    }
}
