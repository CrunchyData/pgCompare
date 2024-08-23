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

import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.services.ColumnValidation.*;
import static com.crunchydata.services.dbCommon.ShouldQuoteString;
import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Utility class for interacting with MySQL databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 * <p>
 *     MySQL Data Types
 *         Date/Time: date, datetime, timestamp, time, year
 *         Numeric: integer, smallint, decimal, numeric, float, real, double, int, dec, fixed
 *         String: char, varchar, text, json
 *         Unsupported: bit, binary, varbinary, blob, enum, set
 *
 * @author Brian Pace
 */
public class dbMySQL {

    private static final String THREAD_NAME = "dbMySQL";

    /**
     * Builds a SQL query for loading data from a MySQL table.
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
            sql += "lower(md5(" + pkColumns + ")) pk_hash, " + pkJSON + " pk, lower(md5(" + columns + ")) column_hash FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        }

        if ( tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }

    /**
     * Generates a column value expression for MySQL based on the column's data type.
     *
     * @param column JSONObject containing column information.
     * @return String representing the column value expression.
     */
    public static String columnValueMapMySQL(JSONObject column) {
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            switch (column.getString("dataType").toLowerCase()) {
                case "float":
                case "double":
                    colExpression = "coalesce(if(" + column.getString("columnName") + "=0,'0.000000e+00',concat(if(" + column.getString("columnName") + "<0, '-', ''),format(abs(" + column.getString("columnName") + ")/pow(10, floor(log10(abs(" + column.getString("columnName") + ")))), 6),'e',if(floor(log10(abs(" + column.getString("columnName") + ")))>=0,'+','-'),lpad(replace(replace(convert(FORMAT(floor(log10(abs(" + column.getString("columnName") + "))), 2)/100,char),'0.',''),'-',''),2,'0'))),' ')";
                    break;
                default:
                    if (Props.getProperty("number-cast").equals("notation")) {
                        colExpression = "coalesce(if(" + column.getString("columnName") + "=0,'0.0000000000e+00',concat(if(" + column.getString("columnName") + "<0, '-', ''),format(abs(" + column.getString("columnName") + ")/pow(10, floor(log10(abs(" + column.getString("columnName") + ")))), 10),'e',if(floor(log10(abs(" + column.getString("columnName") + ")))>=0,'+','-'),lpad(replace(replace(convert(FORMAT(floor(log10(abs(" + column.getString("columnName") + "))), 2)/100,char),'0.',''),'-',''),2,'0'))),' ')";
                    } else {
                        colExpression = "coalesce(if(instr(convert(" + column.getString("columnName") + ",char),'.')>0,concat(if(" + column.getString("columnName") + "<0,'-',''),lpad(substring_index(convert(abs(" + column.getString("columnName") + "),char),'.',1),22,'0'),'.',rpad(substring_index(convert(" + column.getString("columnName") + ",char),'.',-1),22,'0')),concat(if(" + column.getString("columnName") + "<0,'-',''),lpad(convert(" + column.getString("columnName") + ",char),22,'0'),'.',rpad('',22,'0'))),' ')";
                    }
            }
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(convert(" + column.getString("columnName") + ",char),'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("timestamp") || column.getString("dataType").toLowerCase().contains("datetime") ) {
                colExpression = "coalesce(date_format(convert_tz(" + column.getString("columnName") + ",@@session.time_zone,'UTC'),'%m%d%Y%H%i%S'),' ')";
            } else {
                colExpression = "coalesce(date_format(" + column.getString("columnName") + ",'%m%d%Y%H%i%S'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(" + column.getString("columnName") + ",' ')";
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(md5(" + column.getString("columnName") +"), ' ')";
        } else {
            colExpression = column.getString("columnName");
        }

        return colExpression;

    }

    /**
     * Retrieves column metadata for a specified table in MySQL database.
     *
     * @param conn   Database connection to MySQL server.
     * @param schema Schema name of the table.
     * @param table  Table name.
     * @return JSONArray containing metadata for each column in the table.
     */
    public static JSONArray getColumns (Connection conn, String schema, String table) {
        JSONArray columnInfo = new JSONArray();

        try {
            PreparedStatement stmt = conn.prepareStatement(SQL_MYSQL_SELECT_COLUMNS);
            stmt.setObject(1, schema);
            stmt.setObject(2,table);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                JSONObject column = new JSONObject();
                if ( Arrays.asList(unsupportedDataTypes).contains(rs.getString("data_type").toLowerCase()) ) {
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
                column.put("nullable",rs.getString("nullable").equals("Y"));
                column.put("primaryKey",rs.getString("pk").equals("Y"));
                column.put("valueExpression", columnValueMapMySQL(column));
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));

                columnInfo.put(column);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error retrieving columns for table %s.%s:  %s",schema, table, e.getMessage()));
        }
        return columnInfo;
    }

    /**
     * Establishes a connection to a MySQL database using the provided connection properties.
     *
     * @param connectionProperties Properties containing database connection information.
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to MySQL database.
     */
    public static Connection getConnection(Properties connectionProperties, String destType) {
        Connection conn = null;
        String url = "jdbc:mysql://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname")+"?allowPublicKeyRetrieval=true&useSSL="+(connectionProperties.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
            dbCommon.simpleUpdate(conn,"set session sql_mode='ANSI'", new ArrayList<>(), false);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error connecting to MySQL:  %s", e.getMessage()));
        }

        return conn;

    }

}
