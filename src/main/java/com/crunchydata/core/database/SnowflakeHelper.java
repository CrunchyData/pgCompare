package com.crunchydata.core.database;

import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static com.crunchydata.util.JsonProcessingUtils.findOne;
import static com.crunchydata.util.JsonProcessingUtils.replaceObjectAtLocation;

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
