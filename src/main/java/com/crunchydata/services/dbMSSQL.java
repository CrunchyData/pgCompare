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
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.services.ColumnValidation.*;
import static com.crunchydata.services.dbCommon.ShouldQuoteString;
import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Utility class for interacting with Microsoft SQL Server databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 * <p>
 *     MSSQL Data Types
 *         Date/Time: datetime, smalldatetime, date, time, datetimeoffset, datetime2
 *         Numeric: bigint, int, smallint, tinyint, decimal, numeric, money, smallmoney
 *         String: char, varchar, text, nchar, nvarchar, ntext, xml
 *         Unsupported: bit, binary, varbinary, image, cursor, rowversion, hierarchyid, uniqueidentifier, sql_variant
 *
 * @author Brian Pace
 */
public class dbMSSQL {
    public static final String nativeCase = "lower";

    private static final String THREAD_NAME = "dbMSSQL";

    /**
     * Builds a SQL query for loading data from a SQL Server table.
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
            sql += "lower(convert(varchar, hashbytes('MD5'," + pkColumns + "),2)) pk_hash, " + pkJSON + " pk, lower(convert(varchar, hashbytes('MD5'," + columns + "),2)) column_hash FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        } else {
            sql += pkColumns + " pk_hash, " + pkJSON + " pk, " + columns + " FROM " + ShouldQuoteString(schema) + "." + ShouldQuoteString(tableName) + " WHERE 1=1 ";
        }

        if ( tableFilter != null && !tableFilter.isEmpty()) {
            sql += " AND " + tableFilter;
        }

        return sql;
    }

    /**
     * Generates a column value expression for MSSQL based on the column's data type.
     *
     * @param column JSONObject containing column information.
     * @return String representing the column value expression.
     */
    public static String columnValueMapMSSQL(JSONObject column) {
        String colExpression;

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression =   Props.getProperty("number-cast").equals("notation") ? "lower(replace(coalesce(trim(to_char(" + column.getString("columnName") + ",'E10')),' '),'E+0,'e+'))"   : "coalesce(cast(format(" + column.getString("columnName") + ",'0000000000000000000000.0000000000000000000000') as text),' ')";
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(cast(" + column.getString("columnName") + " as varchar),'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(format(" + column.getString("columnName") + ",MMddyyyyHHMIss'),' ')";
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(" + column.getString("columnName") + ",' ')";
        } else {
            colExpression = column.getString("columnName");
        }

        return colExpression;

    }

    /**
     * Retrieves column metadata for a specified table in MSSQL database.
     *
     * @param conn   Database connection to MSSQL server.
     * @param schema Schema name of the table.
     * @param table  Table name.
     * @return JSONArray containing metadata for each column in the table.
     */
    public static JSONArray getColumns (Connection conn, String schema, String table) {
        JSONArray columnInfo = new JSONArray();

        try {
            PreparedStatement stmt = conn.prepareStatement(SQL_MSSQL_SELECT_COLUMNS);
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
                column.put("valueExpression", columnValueMapMSSQL(column));
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
     * Establishes a connection to a MSSQL database using the provided connection properties.
     *
     * @param connectionProperties Properties containing database connection information.
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to MSSQL database.
     */
    public static Connection getConnection(Properties connectionProperties, String destType) {
        Connection conn = null;
        String url = "jdbc:sqlserver://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+";databaseName="+connectionProperties.getProperty(destType+"-dbname")+";encrypt="+(connectionProperties.getProperty(destType+"-sslmode").equals("disable") ? "false" : "true");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error connecting to MSSQL:  %s",e.getMessage()));
        }

        return conn;

    }

}
