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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions.GroupCommitMode;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions.WriterMode;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The validations the options run at construction. Both of them are conditional on the writer mode:
 * what is rejected is a combination of options that cannot work, not an option that some other mode
 * happens to ignore.
 */
public class DorisDataSinkOptionsTest {

    @Test
    public void testBlankLabelPrefixIsRejectedWhereThePrefixBuildsTheLabel() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            assertThatThrownBy(
                            () ->
                                    DorisSinkFixtures.options(
                                            server,
                                            config -> {
                                                config.set(
                                                        DorisDataSinkOptions.WRITER_MODE,
                                                        "stateful");
                                                config.set(DorisDataSinkOptions.LABEL_PREFIX, " ");
                                            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(DorisDataSinkOptions.LABEL_PREFIX.key());
        }
    }

    @Test
    public void testBlankLabelPrefixIsRejectedForTwoPhaseCommit() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            // Group Commit is left at its default so this reaches the prefix check rather than the
            // other validation, which rejects that combination outright.
            assertThatThrownBy(
                            () ->
                                    DorisSinkFixtures.options(
                                            server,
                                            config -> {
                                                config.set(
                                                        DorisDataSinkOptions.WRITER_MODE,
                                                        "stateful-2pc");
                                                config.set(DorisDataSinkOptions.LABEL_PREFIX, "");
                                            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(DorisDataSinkOptions.LABEL_PREFIX.key());
        }
    }

    @Test
    public void testBlankLabelPrefixIsAcceptedUnderGroupCommit() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            // Group Commit loads carry no label (a label would degrade them to non-Group-Commit),
            // so the writer never reads the prefix and a blank one breaks nothing.
            DorisDataSinkOptions options =
                    DorisSinkFixtures.options(
                            server,
                            config -> {
                                config.set(DorisDataSinkOptions.WRITER_MODE, "stateful");
                                config.set(
                                        DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                        GroupCommitMode.SYNC_MODE);
                                config.set(DorisDataSinkOptions.LABEL_PREFIX, "");
                            });

            assertThat(options.getLabelPrefix()).isEmpty();
        }
    }

    @Test
    public void testBlankLabelPrefixIsAcceptedForTheLegacyWriter() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            // The legacy writer labels its loads with its own built-in prefix and never reads this
            // option — it is the frozen write path, unchanged by the option's introduction.
            DorisDataSinkOptions options =
                    DorisSinkFixtures.options(
                            server, config -> config.set(DorisDataSinkOptions.LABEL_PREFIX, ""));

            assertThat(options.getWriterMode()).isEqualTo(WriterMode.LEGACY);
            assertThat(options.getLabelPrefix()).isEmpty();
        }
    }

    @Test
    public void testTwoPhaseCommitAndGroupCommitCannotBeCombined() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            assertThatThrownBy(
                            () ->
                                    DorisSinkFixtures.options(
                                            server,
                                            config -> {
                                                config.set(
                                                        DorisDataSinkOptions.WRITER_MODE,
                                                        "stateful-2pc");
                                                config.set(
                                                        DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                                        GroupCommitMode.SYNC_MODE);
                                            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(DorisDataSinkOptions.WRITER_MODE.key())
                    .hasMessageContaining(DorisDataSinkOptions.GROUP_COMMIT_MODE.key());
        }
    }

    @Test
    public void testTheLabelPrefixDefaultsToCdc() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            Configuration config = new Configuration();
            config.set(DorisDataSinkOptions.FENODES, server.endpoint());
            config.set(DorisDataSinkOptions.USERNAME, "root");
            config.set(DorisDataSinkOptions.PASSWORD, "123456");

            assertThat(new DorisDataSinkOptions(config).getLabelPrefix())
                    .isEqualTo(DorisSinkFixtures.DEFAULT_LABEL_PREFIX);
        }
    }
}
