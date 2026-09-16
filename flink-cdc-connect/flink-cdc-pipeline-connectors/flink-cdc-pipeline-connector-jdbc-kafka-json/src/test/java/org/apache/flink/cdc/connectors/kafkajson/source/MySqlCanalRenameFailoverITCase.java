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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.factory.KafkaJsonDataSourceFactory;
import org.apache.flink.cdc.connectors.kafkajson.infra.CanalServerContainer;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rename under a real failover: MySQL + canal-server + Kafka, a {@code RENAME TABLE} while the
 * job runs, a data change on the renamed table, and then a task failure that takes the job down and
 * makes Flink restart it from its last completed checkpoint.
 *
 * <p>{@link MySqlCanalRenameITCase} shows that a <em>running</em> job keeps the renamed table's
 * changes. This test is about what a <em>restored</em> job does, and what it puts on trial is the
 * state, not the emission. The data change of step 3 has the emitter advance the stream split's
 * position — a schema change does not do that, only a data record or a heartbeat does — so the
 * position the checkpoint carries lies past the DDL, and the restored run drops every message at or
 * before it as a residual of the stream phase ({@code shouldDropAsStreamPhaseResidual}). Nothing
 * about the rename can reach the restored task that way: the only thing that can tell it the
 * table's new name is the checkpointed stream split, which the handler writes — it enqueues the
 * schema-change record whatever {@code include.schema.changes} says — and which the restore
 * re-registers into the store.
 *
 * <p>The job therefore runs with the switch <em>off</em>, the default: no DDL reaches downstream at
 * all, so nothing downstream can stand in for the state, and a job that showed the DDL would have a
 * second channel to learn the name from. The table list likewise names only the table that exists
 * when the job is configured — a job cannot be told a name that does not exist yet — because the
 * emission rule consults that list for a table it cannot place; listing the post-rename name would
 * let the renamed table through on the rule that admits a newly added table, which is a different
 * path from the one a real job takes.
 *
 * <p>The failure is injected rather than provoked: the record that kills the job is thrown on by
 * {@link FailOnceOnARenamedTablesRecord}, and it fails exactly once, so the record is re-read after
 * the restart and has to pass then. Without the fix the restored job either drops the renamed
 * table's records (nothing downstream ever sees them) or throws converting them (the restart loop
 * exhausts and the job ends up FAILED) — both fail this test.
 *
 * <p>The sink and the operator counters are static fields of this class: the MiniCluster runs in
 * the test's JVM, so the task threads share them, while a field captured by a user function would
 * be a deserialized copy inside the task.
 */
public class MySqlCanalRenameFailoverITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCanalRenameFailoverITCase.class);

    private static final String CUSTOMERS = "customers";
    private static final String VIP_CUSTOMERS = "vip_customers";

    /** Every event that reached downstream, in arrival order. */
    private static final BlockingQueue<Event> EVENTS = new LinkedBlockingQueue<>();

    /** How many times the fail-once operator was opened: one per run of the job. */
    private static final AtomicInteger TASK_STARTS = new AtomicInteger();

    /** How many times the injected failure actually fired. */
    private static final AtomicInteger INJECTED_FAILURES = new AtomicInteger();

    /** Armed by the test; consumed by the first record of the renamed table. */
    private static final AtomicBoolean FAIL_ON_NEXT_VIP_RECORD = new AtomicBoolean();

    protected static final MySqlContainer MYSQL8 = createMySqlContainer(MySqlVersion.V8_0);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8, KAFKA)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        KAFKA.stop();
        MYSQL8.stop();
        LOG.info("Containers are stopped.");
    }

    @Test(timeout = 300_000)
    public void testTheRenamedTableSurvivesAFailureAndARestart() throws Exception {
        EVENTS.clear();
        TASK_STARTS.set(0);
        INJECTED_FAILURES.set(0);
        FAIL_ON_NEXT_VIP_RECORD.set(false);

        UniqueDatabase database = new UniqueDatabase(MYSQL8, "customers", TEST_USER, TEST_PASSWORD);
        database.createAndInitialize();
        String dbName = database.getDatabaseName();
        String topic = "canal-rename-failover-" + UUID.randomUUID();

        CanalServerContainer canal = new CanalServerContainer(dbName, topic, NETWORK, LOG);
        canal.start();
        canal.waitUntilStarted();
        // let the instance finish registering as a slave and start tailing the binlog
        Thread.sleep(3_000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnv(env);
        // the failover under test: the job has to come back from its checkpoint on its own
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(0)));

        KafkaJsonSourceConfigFactory configFactory =
                buildConfigFactory(
                                database.getHost(),
                                database.getDatabasePort(),
                                TEST_USER,
                                TEST_PASSWORD,
                                dbName,
                                // Only the name that exists when the job is configured: a job
                                // cannot be told about a table name that does not exist yet, and
                                // the filter is what the emission rule consults for an unknown
                                // table. Listing the post-rename name as well would let the
                                // renamed table through on the rule that admits a newly added
                                // table, which is a different path from the one a real job takes
                                CUSTOMERS,
                                KAFKA.getBootstrapServers(),
                                topic)
                        // the default, and the configuration the state leg is load-bearing in:
                        // with the DDL withheld from downstream, the checkpointed split state is
                        // the only channel the restored run can learn the table's name from
                        .includeSchemaChanges(false);

        KafkaJsonDataSource dataSource = new KafkaJsonDataSource(configFactory);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();
        env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        KafkaJsonDataSourceFactory.IDENTIFIER,
                        new KafkaJsonEventTypeInfo())
                .map(new FailOnceOnARenamedTablesRecord())
                .returns(new KafkaJsonEventTypeInfo())
                .addSink(new CollectingSink());

        JobClient job = env.executeAsync("kafka-json-rename-failover");
        try {
            // 1) the snapshot, under the original name
            awaitEvents(
                    "the 4 snapshot rows of " + CUSTOMERS,
                    120_000L,
                    events -> dataChanges(events, CUSTOMERS).size() >= 4);
            // let the snapshot phase finish (high-watermark capture + stream split assignment)
            Thread.sleep(5_000);

            // 2) the rename
            execute(
                    database,
                    String.format(
                            "RENAME TABLE `%s`.`%s` TO `%s`.`%s`",
                            dbName, CUSTOMERS, dbName, VIP_CUSTOMERS));

            // 3) one data change on the renamed table, then the checkpoint that carries its
            //    position. This is what puts the DDL behind the resume point: the emitter advances
            //    the split's position on a data record, so the position checkpointed here is past
            //    the DDL and the restored run drops the DDL (and this record) as a stream-phase
            //    residual instead of re-applying the rename. Seeing this record also says the
            //    rename itself was applied — it is an insert into a table that did not exist a
            //    moment ago
            execute(
                    database,
                    String.format(
                            "INSERT INTO `%s`.`%s` (id, name, address)"
                                    + " VALUES (105, 'user_5', 'Chengdu')",
                            dbName, VIP_CUSTOMERS));
            awaitEvents(
                    "the renamed table's first record",
                    120_000L,
                    events -> dataChanges(events, VIP_CUSTOMERS).size() >= 1);
            Thread.sleep(6_000);

            // 4) the failure: armed just before the DML, so the first record to arrive after it is
            //    the one that takes the job down
            FAIL_ON_NEXT_VIP_RECORD.set(true);
            execute(
                    database,
                    String.format(
                            "UPDATE `%s`.`%s` SET address='Hangzhou' WHERE id=101",
                            dbName, VIP_CUSTOMERS));
            execute(
                    database,
                    String.format("DELETE FROM `%s`.`%s` WHERE id=102", dbName, VIP_CUSTOMERS));

            // 5) after the restart the source resumes from its checkpoint and re-reads the record
            //    that killed the job — it has to convert and be emitted this time, which needs the
            //    new name and the schema to have survived the restart. A count below the expected
            //    one is what a lost record looks like; more would be a duplicate, and the assertion
            //    below is where both are judged
            awaitEvents(
                    "the renamed table's records to come back after the restart",
                    120_000L,
                    events -> dataChanges(events, VIP_CUSTOMERS).size() >= 3);

            List<Event> events = new ArrayList<>(EVENTS);

            // It failed over exactly once, and it recovered: the failure is the test's own, and the
            // operator was opened twice — once per run. A restored job that could not handle the
            // renamed table would have failed again here (or the job would be FAILED)
            assertThat(INJECTED_FAILURES.get()).isEqualTo(1);
            assertThat(TASK_STARTS.get())
                    .as("the operator runs once per run of the job: the original and the restored")
                    .isEqualTo(2);
            assertThat(job.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);

            // Nothing was lost, nothing arrived twice: the four snapshot rows, the renamed table's
            // insert/update/delete, and nothing else — no DDL among them, since the switch is off.
            // The snapshot rows and the stream-phase residual are the load-bearing part: a restored
            // run that had re-read either (a snapshot that was not remembered as finished, a record
            // at or before the checkpointed position) would show up here as a duplicate
            assertThat(dataChanges(events)).containsExactlyInAnyOrderElementsOf(expected(dbName));
            assertThat(events)
                    .as("the DDL stays internal with the schema-change switch off")
                    .noneMatch(event -> event instanceof RenameTableEvent);
        } finally {
            job.cancel().get(10, TimeUnit.SECONDS);
            canal.stop();
        }
    }

    /** The data changes the scenario produces: the snapshot rows, then the renamed table's DML. */
    private static List<Event> expected(String dbName) {
        List<Event> expected = new ArrayList<>(expectedSnapshotEvents(dbName));
        BinaryRecordDataGenerator generator = snapshotRowGenerator();
        TableId renamed = TableId.tableId(dbName, VIP_CUSTOMERS);
        expected.add(
                DataChangeEvent.insertEvent(
                        renamed,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("user_5"),
                                    BinaryStringData.fromString("Chengdu")
                                })));
        expected.add(
                DataChangeEvent.updateEvent(
                        renamed,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("user_1"),
                                    BinaryStringData.fromString("Shanghai")
                                }),
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("user_1"),
                                    BinaryStringData.fromString("Hangzhou")
                                })));
        expected.add(
                DataChangeEvent.deleteEvent(
                        renamed,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("user_2"),
                                    BinaryStringData.fromString("Beijing")
                                })));
        return expected;
    }

    // ---------------------------------------------------------------------------------------------
    // the running job's operators, observed through static state
    // ---------------------------------------------------------------------------------------------

    /**
     * Fails exactly once, on the first record of the renamed table. Fail-once rather than always:
     * the record is re-read after the restart and must pass then — otherwise every restart attempt
     * would fail on the same record and the job would end up FAILED, which is the other half of
     * what this test rules out.
     */
    private static final class FailOnceOnARenamedTablesRecord
            extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(Configuration parameters) {
            TASK_STARTS.incrementAndGet();
        }

        @Override
        public Event map(Event event) {
            if (event instanceof DataChangeEvent
                    && VIP_CUSTOMERS.equals(((DataChangeEvent) event).tableId().getTableName())
                    && FAIL_ON_NEXT_VIP_RECORD.compareAndSet(true, false)) {
                INJECTED_FAILURES.incrementAndGet();
                throw new IllegalStateException(
                        "injected failure: the job dies on the first record of "
                                + VIP_CUSTOMERS
                                + " after the rename");
            }
            return event;
        }
    }

    /** Collects every event that reaches downstream. */
    private static final class CollectingSink implements SinkFunction<Event> {

        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(Event event, Context context) {
            EVENTS.add(event);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static List<Event> dataChanges(List<Event> events) {
        return events.stream()
                .filter(event -> event instanceof DataChangeEvent)
                .collect(Collectors.toList());
    }

    private static List<Event> dataChanges(List<Event> events, String table) {
        return dataChanges(events).stream()
                .filter(event -> table.equals(((DataChangeEvent) event).tableId().getTableName()))
                .collect(Collectors.toList());
    }

    /** Polls the events the sink has collected, so the test observes the job while it runs. */
    private static void awaitEvents(String what, long timeoutMs, Predicate<List<Event>> condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test(new ArrayList<>(EVENTS))) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Timed out after "
                        + timeoutMs
                        + " ms waiting for "
                        + what
                        + "; downstream saw "
                        + new ArrayList<>(EVENTS));
    }

    /** Runs one statement against the source database as the test user. */
    private static void execute(UniqueDatabase database, String sql) throws Exception {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
