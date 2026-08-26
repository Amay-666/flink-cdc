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

package org.apache.flink.cdc.connectors.kafkajson.reconcile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates a real workload against the source table over JDBC and records every mutation in the
 * {@link Ledger} as ground truth.
 *
 * <p>The driver is the "source of truth" for what the database actually did: it executes plain SQL
 * on the source (MySQL/TiDB), maintains an in-memory expected model of the table state, and appends
 * one {@link LedgerEntry} per successful DML. It never goes through the connector, so the later
 * reconciliation in {@link LedgerVerifier} compares two independent records of the same mutations.
 *
 * <p>Typical usage: {@link #initSchema()} + {@link #insertInitialRows(int)} to seed the
 * pre-snapshot baseline, then {@link #runRandomDml(int)} once the snapshot phase has finished. The
 * driver runs in its own thread from the perspective of the Flink job; all state it exposes is
 * thread-safe.
 */
public class WorkloadDriver implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkloadDriver.class);

    private final Connection connection;
    private final String database;
    private final String table;
    private final Ledger ledger;
    private final Random random;
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final ConcurrentHashMap<Integer, CustomerRow> expected = new ConcurrentHashMap<>();

    public WorkloadDriver(
            String jdbcUrl,
            String username,
            String password,
            String database,
            String table,
            Ledger ledger,
            long seed)
            throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        this.database = database;
        this.table = table;
        this.ledger = ledger;
        this.random = new Random(seed);
    }

    /** Creates the database and the {@code customers} table if they do not exist yet. */
    public void initSchema() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + database + "`");
            statement.execute(
                    "CREATE TABLE IF NOT EXISTS `"
                            + database
                            + "`.`"
                            + table
                            + "` (id INT NOT NULL, name VARCHAR(255) NOT NULL, "
                            + "address VARCHAR(255), PRIMARY KEY (id))");
        }
    }

    /**
     * Seeds {@code count} rows as the pre-snapshot baseline. Each row is recorded with phase {@link
     * LedgerEntry.Phase#INITIAL}, so the verifier expects them to be delivered by the JDBC snapshot
     * and never as dedicated stream events.
     */
    public void insertInitialRows(int count) throws SQLException {
        for (int i = 0; i < count; i++) {
            int id = nextId.getAndIncrement();
            CustomerRow row = randomRow(id);
            executeInsert(row);
            expected.put(id, row);
            ledger.recordInitialRow(row);
        }
        LOG.info("Inserted {} initial rows (ids 1..{})", count, count);
    }

    /**
     * Executes {@code ops} random DML operations with the given mix (percentages must sum to 100).
     * Every operation is recorded in the ledger with phase {@link LedgerEntry.Phase#POST_SNAPSHOT}.
     */
    public void runRandomDml(int ops, int insertPct, int updatePct) throws SQLException {
        for (int i = 0; i < ops; i++) {
            int roll = random.nextInt(100);
            if (roll < insertPct) {
                insertRandomRow();
            } else if (roll < insertPct + updatePct) {
                updateRandomRow();
            } else {
                deleteRandomRow();
            }
        }
        LOG.info(
                "Finished {} random DML ops (insert {}% / update {}%). Expected size now {}",
                ops, insertPct, updatePct, expected.size());
    }

    /** Executes {@code ops} random DML with a default 40/40/20 insert/update/delete mix. */
    public void runRandomDml(int ops) throws SQLException {
        runRandomDml(ops, 40, 40);
    }

    private void insertRandomRow() throws SQLException {
        int id = nextId.getAndIncrement();
        CustomerRow row = randomRow(id);
        executeInsert(row);
        expected.put(id, row);
        ledger.recordInsert(row);
    }

    private void updateRandomRow() throws SQLException {
        Integer id = randomExistingKey();
        if (id == null) {
            return;
        }
        CustomerRow row = randomRow(id);
        try (java.sql.PreparedStatement ps =
                connection.prepareStatement(
                        "UPDATE `"
                                + database
                                + "`.`"
                                + table
                                + "` SET name=?, address=? WHERE id=?")) {
            ps.setString(1, row.name());
            ps.setString(2, row.address());
            ps.setInt(3, id);
            ps.executeUpdate();
        }
        expected.put(id, row);
        ledger.recordUpdate(row);
    }

    private void deleteRandomRow() throws SQLException {
        Integer id = randomExistingKey();
        if (id == null) {
            return;
        }
        CustomerRow deleted = expected.remove(id);
        try (java.sql.PreparedStatement ps =
                connection.prepareStatement(
                        "DELETE FROM `" + database + "`.`" + table + "` WHERE id=?")) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
        ledger.recordDelete(deleted);
    }

    private void executeInsert(CustomerRow row) throws SQLException {
        try (java.sql.PreparedStatement ps =
                connection.prepareStatement(
                        "INSERT INTO `"
                                + database
                                + "`.`"
                                + table
                                + "` (id, name, address) VALUES (?, ?, ?)")) {
            ps.setInt(1, row.id());
            ps.setString(2, row.name());
            ps.setString(3, row.address());
            ps.executeUpdate();
        }
    }

    private CustomerRow randomRow(int id) {
        String name = "user_" + random.nextInt(100_000);
        String address = random.nextBoolean() ? "addr_" + random.nextInt(100_000) : null;
        return new CustomerRow(id, name, address);
    }

    private Integer randomExistingKey() {
        if (expected.isEmpty()) {
            return null;
        }
        List<Integer> keys = new ArrayList<>(expected.keySet());
        return keys.get(random.nextInt(keys.size()));
    }

    /** The driver's in-memory expected model: current {@code id -> row} for every live row. */
    public ConcurrentHashMap<Integer, CustomerRow> expectedModel() {
        return expected;
    }

    /** Queries the actual final state of the source table, ordered by id. */
    public List<CustomerRow> querySourceFinalState() throws SQLException {
        List<CustomerRow> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT id, name, address FROM `"
                                        + database
                                        + "`.`"
                                        + table
                                        + "` ORDER BY id")) {
            while (rs.next()) {
                rows.add(new CustomerRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
        }
        return rows;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
