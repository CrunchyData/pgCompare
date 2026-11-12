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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the column difference detail built by check (#112). No database.
 *
 * @author pgCompare
 */
class DataValidationThreadColumnDifferenceTest {

    /**
     * Values that made the previous string-concatenation parse throw, which reported the
     * row as in sync, plus values it already handled.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "a, b", "11/11/2026", "http://x.y", "<raiz><item id=\"1\">ok</item></raiz>", "1.000,00",
            "a:b", "a;b", "a=b", "a#b", "{x}", "x \"y\"", "C:\\dir", "tab\there", "line\nbreak",
            "abc", "abc def", "[x]", "a'b", "100%", "-1.5", "1e5", "0012", "true", "null", " ", ""
    })
    void anyValueIsKeptVerbatim(String value) {
        JSONObject diff = DataValidationThread.columnDifference(value, value + "!");

        assertEquals(value, diff.getString("source"));
        assertEquals(value + "!", diff.getString("target"));

        // The detail is stored and logged as text; it must round-trip unchanged.
        JSONObject reparsed = new JSONArray(new JSONArray().put(new JSONObject().put("c", diff)).toString())
                .getJSONObject(0).getJSONObject("c");
        assertEquals(value, reparsed.getString("source"));
        assertEquals(value + "!", reparsed.getString("target"));
    }

    @Test
    void nullValuesAreReportedAsJsonNull() {
        JSONObject diff = DataValidationThread.columnDifference(null, "x");

        assertTrue(diff.isNull("source"));
        assertEquals("x", diff.getString("target"));
    }
}
