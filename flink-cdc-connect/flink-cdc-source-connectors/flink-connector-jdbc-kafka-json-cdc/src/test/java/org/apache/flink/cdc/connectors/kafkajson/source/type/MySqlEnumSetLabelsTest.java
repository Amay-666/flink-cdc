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

package org.apache.flink.cdc.connectors.kafkajson.source.type;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for {@link MySqlEnumSetLabels}. */
class MySqlEnumSetLabelsTest {

    @Test
    void testParsesEnumAndSetExpressions() {
        assertEquals(Arrays.asList("a", "b", "c"), MySqlEnumSetLabels.parse("enum('a','b','c')"));
        assertEquals(Arrays.asList("x", "y"), MySqlEnumSetLabels.parse("set('x','y')"));
    }

    @Test
    void testHandlesEscapesAndCommasInsideLabels() {
        // a doubled quote inside a label is the SQL escape
        assertEquals(Arrays.asList("red", "it's"), MySqlEnumSetLabels.parse("enum('red','it''s')"));
        // a label may contain a comma — a plain split(",") would break it
        assertEquals(Arrays.asList("a,b", "c"), MySqlEnumSetLabels.parse("enum('a,b','c')"));
    }

    @Test
    void testReturnsNullForAnythingElse() {
        assertNull(MySqlEnumSetLabels.parse("varchar(255)"));
        assertNull(MySqlEnumSetLabels.parse("int"));
        assertNull(MySqlEnumSetLabels.parse("enum()"));
        assertNull(MySqlEnumSetLabels.parse("enum"));
        assertNull(MySqlEnumSetLabels.parse(null));
    }
}
