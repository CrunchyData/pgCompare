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

package com.crunchydata.core.database;

import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static com.crunchydata.util.JsonProcessingUtils.findOne;
import static com.crunchydata.util.JsonProcessingUtils.replaceObjectAtLocation;

/**
 * Helper class for supporting methods required for Snowflake comparison.
 *
 * @author Brian Pace
 */
public class SnowflakeHelper {

    private static final String THREAD_NAME = "snowflake-helper";
    private static final String COLUMN_NAME_PROPERTY = "columnName";
    private static final String PRIMARY_KEY_PROPERTY = "primaryKey";

    public static JSONArray GetSnowflakePrimaryKey(Connection conn, JSONArray columnInfo, String catalog, String schema, String table) {
        try {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getPrimaryKeys(catalog.toUpperCase(), schema, table);

            while (rs.next()) {

                String columnName = rs.getString("COLUMN_NAME");

                JSONObject columnResult = findOne(columnInfo, COLUMN_NAME_PROPERTY, columnName);
                JSONObject column = columnResult.getJSONObject("data");
                column.put(PRIMARY_KEY_PROPERTY, true);

                columnInfo = replaceObjectAtLocation(columnInfo, column, columnResult.getInt("location"));

            }

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error during primary key discovery for Snowflake on table %s: %s", table, e.getMessage()));
        }

        return columnInfo;

    }
}
