/*
 * Copyright 2012-2024 the original author or authors.
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

package com.crunchydata.util;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utility class for JSON operations.
 * Provides methods to search for specific key-value pairs within JSON arrays.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public class JsonUtility {

    // Private constructor to prevent instantiation
    private JsonUtility() {
        throw new UnsupportedOperationException("JsonUtility is a utility class and cannot be instantiated.");
    }

    // Constants for better maintainability
    private static final String CHAR_DATA_CLASS = "char";
    private static final String MSSQL_PLATFORM = "mssql";
    private static final String QUOTE_REPLACEMENT = "";
    private static final String JSON_KEY_FORMAT = "'\"%s\": \"' %s %s %s '\"' ";
    private static final String JSON_KEY_FORMAT_MSSQL = "'\"%s\": ' %s trim(cast(%s as varchar))";
    private static final String JSON_KEY_FORMAT_DEFAULT = "'\"%s\": ' %s %s";

    /**
     * Build an expression that can be used in a SQL statement to construct a JSON object
     *
     * @param platform          Database platform
     * @param column            Column name
     * @param dataClass         Class of the data type
     * @param concatOperator    Operator to use for concatenation
     * @return                  Expression for SQL statement
     */
    public static String buildJsonExpression(String platform, String column, String dataClass, String concatOperator) {
        String cleanColumn = column.replace("\"", QUOTE_REPLACEMENT);
        
        if (CHAR_DATA_CLASS.equals(dataClass)) {
            return String.format(JSON_KEY_FORMAT, cleanColumn, concatOperator, column, concatOperator);
        } else if (MSSQL_PLATFORM.equals(platform)) {
            return String.format(JSON_KEY_FORMAT_MSSQL, cleanColumn, concatOperator, column);
        } else {
            return String.format(JSON_KEY_FORMAT_DEFAULT, cleanColumn, concatOperator, column);
        }
    }

    /**
     * Searches for a JSONObject within a JSONArray that contains a specified key-value pair.
     *
     * @param jsonArray the JSONArray to search
     * @param key the key to search for within each JSONObject
     * @param value the value to match for the specified key
     * @return a JSONObject containing the search result with "data", "count", and "location" keys:
     *         - "data": the found JSONObject (if any)
     *         - "count": 1 if a matching JSONObject is found, 0 otherwise
     *         - "location": the index of the matching JSONObject in the JSONArray
     */
    public static JSONObject findOne (JSONArray jsonArray, String key, String value) {
        // Result JSONObject to store search results
        JSONObject result = new JSONObject();
        result.put("count",0);

        // Iterate through the JSONArray
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject currentObject = jsonArray.getJSONObject(i);
            if (currentObject.getString(key).equalsIgnoreCase(value)) {
                result.put("data", currentObject);
                result.put("count", 1);
                result.put("location", i);
                break;
            }
        }

        return result;

    }

}
