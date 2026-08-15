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

import java.util.Objects;

/**
 * A single row of the {@code customers(id INT PK, name VARCHAR, address VARCHAR)} table used by the
 * exactly-once verification scenarios.
 *
 * <p>This is the shared value type of the ground-truth ledger ({@link WorkloadDriver} writes it
 * after each DML) and of the event archive (extracted from {@link
 * org.apache.flink.cdc.common.event.DataChangeEvent#after()} / {@code before()} {@link
 * org.apache.flink.cdc.common.data.RecordData}) — both sides must produce the same value for the
 * reconciliation to be meaningful.
 */
public class CustomerRow {

    private final int id;
    private final String name;
    private final String address;

    public CustomerRow(int id, String name, String address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String address() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CustomerRow)) {
            return false;
        }
        CustomerRow that = (CustomerRow) o;
        return id == that.id
                && Objects.equals(name, that.name)
                && Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, address);
    }

    @Override
    public String toString() {
        return "CustomerRow{id=" + id + ", name='" + name + "', address='" + address + "'}";
    }
}
