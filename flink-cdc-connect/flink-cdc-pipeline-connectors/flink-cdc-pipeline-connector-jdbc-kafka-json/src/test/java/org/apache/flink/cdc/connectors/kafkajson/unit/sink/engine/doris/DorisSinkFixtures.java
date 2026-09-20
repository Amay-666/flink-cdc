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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * The scaffolding the stateful-writer tests share: a hand-stubbed {@link Sink.InitContext}, the
 * options builder that points a writer at a {@link MockDorisServer}, and the two events a row needs
 * before it can be written.
 *
 * <p>{@link DorisSinkWriterTest} keeps its own copies of the same helpers and is left as it is: it
 * is the released writer's regression baseline, and rewriting it while adding the stateful writers
 * would blur what that baseline covers. The duplication is the price of that separation, and it is
 * confined to this one file for the new tests.
 */
public final class DorisSinkFixtures {

    public static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    public static final TableId ORDERS = TableId.tableId("shop", "orders");
    public static final Schema ORDERS_SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    /** The label prefix the options default to. */
    public static final String DEFAULT_LABEL_PREFIX = "cdc";

    private DorisSinkFixtures() {}

    /** A server that accepts every load, which is all a test asserting a request needs. */
    public static MockDorisServer successServer() throws IOException {
        return new MockDorisServer(request -> Response.ok("{\"Status\":\"Success\"}"));
    }

    /** The options every test starts from: a mock endpoint and no periodic flush timer. */
    public static DorisDataSinkOptions options(MockDorisServer server) {
        return options(server, config -> {});
    }

    public static DorisDataSinkOptions options(
            MockDorisServer server, Consumer<Configuration> tune) {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, server.endpoint());
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        // Disable the periodic flush timer by default; individual tests opt back in.
        config.set(DorisDataSinkOptions.FLUSH_INTERVAL, Duration.ZERO);
        tune.accept(config);
        return new DorisDataSinkOptions(config);
    }

    /** The {@code CreateTableEvent} a table needs before rows for it are accepted. */
    public static CreateTableEvent createOrdersEvent() {
        return new CreateTableEvent(ORDERS, ORDERS_SCHEMA);
    }

    public static DataChangeEvent insertEvent(int id) {
        return DataChangeEvent.insertEvent(ORDERS, record(id));
    }

    public static BinaryRecordData record(Object... values) {
        return new BinaryRecordDataGenerator(
                        RowType.of(
                                ORDERS_SCHEMA.getColumns().stream()
                                        .map(column -> column.getType())
                                        .toArray(DataType[]::new)))
                .generate(values);
    }

    /**
     * A minimal {@link Sink.InitContext} stub with the two fields the stateful writers read: the
     * subtask id, which is part of every two-phase label, and the restored checkpoint id, which is
     * the epoch a restored writer starts from. The time service is null unless a test sets one, and
     * the metric group is null as in {@link DorisSinkWriterTest} (the metrics class tolerates it).
     */
    public static class FakeInitContext implements Sink.InitContext {

        private final ProcessingTimeService timeService;
        private final int subtaskId;
        private final long restoredCheckpointId;

        public FakeInitContext() {
            this(null, 0, null);
        }

        public FakeInitContext(ProcessingTimeService timeService) {
            this(timeService, 0, null);
        }

        public FakeInitContext(
                ProcessingTimeService timeService, int subtaskId, Long restoredCheckpointId) {
            this.timeService = timeService;
            this.subtaskId = subtaskId;
            this.restoredCheckpointId = restoredCheckpointId == null ? -1L : restoredCheckpointId;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return subtaskId;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId < 0
                    ? OptionalLong.empty()
                    : OptionalLong.of(restoredCheckpointId);
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return null;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        @Override
        public JobID getJobId() {
            return null;
        }
    }
}
