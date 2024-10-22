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

package com.crunchydata.services;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import com.crunchydata.util.Logging;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.services.ColumnValidation.*;
import static com.crunchydata.services.dbCommon.ShouldQuoteString;
import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Utility class for interacting with Oracle databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 *
 *
 * @author Brian Pace
 */
public class dbOracle {

    private static final String THREAD_NAME = "dbOracle";

    /**
     * Builds a SQL query for loading data from a Oracle table.
     *
     * @param useDatabaseHash Whether to use MD5 hash for database columns.
     * @param schema          Schema name of the table.
     * @param tableName       Name of the table.
     * @param pkColumns       Columns used as primary key.
     * @param pkJSON          JSON representation of primary key columns.
     * @param columns         Columns to select from the table.
     * @param tableFilter     Optional filter condition for the WHERE clause.
     * @return SQL query string for loading data from the specified table.
     */
    public static String buildLoadSQL (Boolean useDatabaseHash, String schema, String tableName, String pkColumns, String pkJSON, String columns, String tableFilter) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "LOWER(STANDARD_HASH(" + pkColumns + ",'MD5')) pk_hash, " + pkJSON + " pk, LOWER(STANDARD_HASH(" + columns + ",'MD5')) column_hash FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        }

        if ( tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }

    /**
     * Generates a column value expression for Oracle based on the column's data type.
     *
     * @param column JSONObject containing column information.
     * @return String representing the column value expression.
     */
    public static String columnValueMapOracle(JSONObject column) {
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {

            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "float", "binary_float", "binary_double" ->
                        "lower(nvl(trim(to_char(" + ShouldQuoteString(column.getString("columnName")) + ",'0.999999EEEE')),' '))";
                default ->
                        Props.getProperty("number-cast").equals("notation") ? "lower(nvl(trim(to_char(" + ShouldQuoteString(column.getString("columnName")) + ",'0.9999999999EEEE')),' '))" : "nvl(trim(to_char(" + ShouldQuoteString(column.getString("columnName")) + ",'0000000000000000000000.0000000000000000000000')),' ')";
            };


        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "nvl(to_char(" + ShouldQuoteString(column.getString("columnName")) + "),'0')";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("time zone") || column.getString("dataType").toLowerCase().contains("tz") ) {
                colExpression = "nvl(to_char(" + ShouldQuoteString(column.getString("columnName")) + " at time zone 'UTC','MMDDYYYYHH24MISS'),' ')";
            } else {
                colExpression = "nvl(to_char(" + ShouldQuoteString(column.getString("columnName")) + ",'MMDDYYYYHH24MISS'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("lob")) {
                colExpression = "nvl(trim(to_char(" + ShouldQuoteString(column.getString("columnName")) + ")),' ')";
            } else {
                if (column.getInt("dataLength") > 1) {
                    colExpression = "nvl(trim(" + ShouldQuoteString(column.getString("columnName")) + "),' ')";
                } else {
                    colExpression = "nvl(" + ShouldQuoteString(column.getString("columnName")) + ",' ')";
                }
            }
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when dbms_lob.getlength(" + ShouldQuoteString(column.getString("columnName")) +") = 0 or " + ShouldQuoteString(column.getString("columnName")) + " is null then ' ' else lower(dbms_crypto.hash(" + ShouldQuoteString(column.getString("columnName")) + ",2)) end";
        } else {
            colExpression = ShouldQuoteString(column.getString("columnName"));
        }

        return colExpression;

    }

    /**
     * Retrieves column metadata for a specified table in Oracle database.
     *
     * @param conn   Database connection to Oracle server.
     * @param schema Schema name of the table.
     * @param table  Table name.
     * @return JSONArray containing metadata for each column in the table.
     */
    public static JSONArray getColumns (Connection conn, String schema, String table) {
        JSONArray columnInfo = new JSONArray();

        try {
            PreparedStatement stmt = conn.prepareStatement(SQL_ORACLE_SELECT_COLUMNS);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                JSONObject column = new JSONObject();
                if (Arrays.asList(unsupportedDataTypes).contains(rs.getString("data_type").toLowerCase()) ) {
                    Logging.write("warning", THREAD_NAME, String.format("Unsupported data type (%s) for column (%s)", rs.getString("data_type"), rs.getString("column_name")));
                    column.put("supported",false);
                } else {
                    column.put("supported",true);
                }
                column.put("columnName",rs.getString("column_name"));
                column.put("dataType",rs.getString("data_type"));
                column.put("dataLength",rs.getInt("data_length"));
                column.put("dataPrecision",rs.getInt("data_precision"));
                column.put("dataScale",rs.getInt("data_scale"));
                column.put("nullable", rs.getString("nullable").equals("Y"));
                column.put("primaryKey",rs.getString("pk").equals("Y"));
                column.put("valueExpression", columnValueMapOracle(column));
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));

                columnInfo.put(column);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error retrieving columns for table %s.%s:  %s",schema,table,e.getMessage()));
        }
        return columnInfo;
    }

    /**
     * Establishes a connection to an Oracle database using the provided connection properties.
     *
     * @param connectionProperties Properties containing database connection information.
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to Oracle database.
     */
    public static Connection getConnection(Properties connectionProperties, String destType) {
        Connection conn = null;
        String url = "jdbc:oracle:thin:@//"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error connecting to Oracle %s",e.getMessage()));
        }

        return conn;

    }

}
