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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit;

/**
 * One pre-committed StreamLoad transaction, handed from the writer to the {@link DorisCommitter}
 * once the checkpoint it belongs to is complete.
 *
 * <p>The transaction id is the whole point: committing is a plain HTTP call against Doris's {@code
 * _stream_load_2pc} endpoint addressed by transaction id, with no SQL and no additional privilege.
 * The database, table and label travel with it because they are what an operator needs to trace a
 * stuck commit back to a batch — Doris reports transactions by id, but a job's logs name batches by
 * table and label.
 */
public class DorisCommittable {

    private final String database;
    private final String table;
    private final String label;
    private final long transactionId;

    public DorisCommittable(String database, String table, String label, long transactionId) {
        this.database = database;
        this.table = table;
        this.label = label;
        this.transactionId = transactionId;
    }

    /** The database the rows were loaded into. */
    public String getDatabase() {
        return database;
    }

    /** The table the rows were loaded into; diagnostic only — the endpoint has no table in it. */
    public String getTable() {
        return table;
    }

    /** The label the rows were pre-committed under; the handle Doris knows the batch by. */
    public String getLabel() {
        return label;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "DorisCommittable{"
                + database
                + '.'
                + table
                + ", label="
                + label
                + ", transactionId="
                + transactionId
                + '}';
    }
}
