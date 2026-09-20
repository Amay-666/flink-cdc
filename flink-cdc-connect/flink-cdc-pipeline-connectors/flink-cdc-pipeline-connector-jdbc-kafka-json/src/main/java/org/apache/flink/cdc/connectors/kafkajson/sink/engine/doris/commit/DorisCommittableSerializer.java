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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializes {@link DorisCommittable}.
 *
 * <p>Worth being careful with: these are what the {@code Committer} operator writes into its own
 * checkpoint, so a committable that cannot be read back is a commit that never happens and rows
 * that never become visible. Only the writer that produced them ever writes them, and only the
 * committer reads them, so the version is the only compatibility contract that matters.
 */
public class DorisCommittableSerializer implements SimpleVersionedSerializer<DorisCommittable> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DorisCommittable committable) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dataOut = new DataOutputStream(out)) {
            dataOut.writeUTF(committable.getDatabase());
            dataOut.writeUTF(committable.getTable());
            dataOut.writeUTF(committable.getLabel());
            dataOut.writeLong(committable.getTransactionId());
            dataOut.flush();
            return out.toByteArray();
        }
    }

    @Override
    public DorisCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported "
                            + DorisCommittable.class.getSimpleName()
                            + " version "
                            + version
                            + "; this connector writes version "
                            + CURRENT_VERSION
                            + ".");
        }
        try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String database = dataIn.readUTF();
            String table = dataIn.readUTF();
            String label = dataIn.readUTF();
            long transactionId = dataIn.readLong();
            return new DorisCommittable(database, table, label, transactionId);
        }
    }
}
