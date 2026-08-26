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

package org.apache.flink.cdc.connectors.kafkajson.source.message;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessageParser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonParserFactory} and {@link CanalMessageParser}. */
class KafkaJsonParserFactoryTest {

    @Test
    void testCanalFactoryCreatesCanalParser() {
        KafkaJsonMessageParser parser = KafkaJsonParserFactory.create(MessageFormat.CANAL);
        assertTrue(parser instanceof CanalMessageParser);
        assertEquals(MessageFormat.CANAL, parser.getFormat());
    }

    @Test
    void testCanalParserParsesFlatMessage() {
        KafkaJsonMessage message =
                KafkaJsonParserFactory.create(MessageFormat.CANAL)
                        .parse(
                                "{\"database\":\"test\",\"table\":\"users\",\"es\":1,\"ts\":2,"
                                        + "\"isDdl\":false,\"type\":\"INSERT\","
                                        + "\"data\":[{\"id\":\"1\"}]}");
        assertTrue(message instanceof CanalMessage);
        CanalMessage flatMessage = (CanalMessage) message;
        assertEquals("test", flatMessage.getDatabase());
        assertEquals("users", flatMessage.getTable());
        assertEquals(1, flatMessage.getData().size());
    }

    @Test
    void testCanalParserToleratesBlankJson() {
        assertNull(KafkaJsonParserFactory.create(MessageFormat.CANAL).parse("  "));
    }

    @Test
    void testDebeziumFactoryCreatesDebeziumParser() {
        KafkaJsonMessageParser parser = KafkaJsonParserFactory.create(MessageFormat.DEBEZIUM);
        assertTrue(parser instanceof DebeziumMessageParser);
        assertEquals(MessageFormat.DEBEZIUM, parser.getFormat());
    }
}
