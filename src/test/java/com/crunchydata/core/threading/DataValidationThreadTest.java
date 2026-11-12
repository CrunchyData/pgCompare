/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.core.threading;

import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the pure helpers of {@link DataValidationThread} (no database).
 *
 * @author pgCompare
 */
class DataValidationThreadTest {

    private static Object bindFor(String pkJson) {
        return DataValidationThread.toPkBindValue(new JSONObject(pkJson).get("id"));
    }

    @Nested
    @DisplayName("Primary key bind values for check (#111)")
    class PkBindValue {

        @ParameterizedTest(name = "{0} binds as {1}")
        @CsvSource(delimiter = '|', value = {
                "{\"id\":2147483647}                    | 2147483647",
                "{\"id\":2147483648}                    | 2147483648",
                "{\"id\":-2147483649}                   | -2147483649",
                "{\"id\":123456789020}                  | 123456789020",
                "{\"id\":9223372036854775807}           | 9223372036854775807",
                "{\"id\":-9223372036854775808}          | -9223372036854775808",
                "{\"id\":99999999999999999999999999999999999999} | 99999999999999999999999999999999999999",
                "{\"id\":10.50}                         | 10.50",
                "{\"id\":-0.0001}                       | -0.0001",
                "{\"id\":12345678901.23}                | 12345678901.23"
        })
        void numericKeysKeepTheirFullValue(String pkJson, String expected) {
            Object value = bindFor(pkJson);
            assertInstanceOf(Number.class, value);
            assertEquals(0, new BigDecimal(expected).compareTo(new BigDecimal(value.toString())),
                    () -> "bound " + value + " for " + pkJson);
        }

        @Test
        @DisplayName("keys wider than 64 bits bind as BigDecimal, which every JDBC driver accepts")
        void bigIntegerBindsAsBigDecimal() {
            assertInstanceOf(BigDecimal.class, bindFor("{\"id\":99999999999999999999999999999999999999}"));
        }

        @Test
        void stringKeysAreUnchanged() {
            assertEquals("ABC-001", bindFor("{\"id\":\"ABC-001\"}"));
        }

        @Test
        void booleanKeysAreUnchanged() {
            assertEquals(Boolean.TRUE, bindFor("{\"id\":true}"));
        }

        @Test
        void jsonNullBindsAsNull() {
            assertNull(bindFor("{\"id\":null}"));
        }
    }
}
