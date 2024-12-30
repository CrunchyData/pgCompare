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


import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.util.Logging;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.util.ColumnUtility.*;
import static com.crunchydata.util.DataUtility.ShouldQuoteString;
import static com.crunchydata.util.Settings.Props;

/**
 * Utility class for interacting with DB2 databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 *
 *
 * @author Brian Pace
 */
public class dbDB2 {
    public static final String nativeCase = "upper";

    private static final String THREAD_NAME = "dbDB2";

    /**
     * Builds a SQL query for loading data from a DB2 table.
     *
     * @param useDatabaseHash Whether to use MD5 hash for database columns.
     * @return SQL query string for loading data from the specified table.
     */
    public static String buildLoadSQL (Boolean useDatabaseHash, DCTableMap tableMap, ColumnMetadata columnMetadata) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "LOWER(HASH(" +  columnMetadata.getPk() + ",'MD5')) pk_hash, " + columnMetadata.getPkJSON() + " pk, LOWER(HASH(" + columnMetadata.getColumn() + ",'MD5')) column_hash FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        } else {
            sql +=  columnMetadata.getPk() + " pk_hash, " + columnMetadata.getPkJSON() + " pk, " + columnMetadata.getColumn() + " FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        }

        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().isEmpty()) {
            sql += " AND " + tableMap.getTableFilter();
        }

        return sql;
    }

    /**
     * Generates a column value expression for DB2 based on the column's data type.
     *
     * @param column JSONObject containing column information.
     * @return String representing the column value expression.
     */
    public static String columnValueMapDB2(JSONObject column) {
        String colExpression;
        String columnName = ShouldQuoteString(column.getBoolean("preserveCase"), column.getString("columnName"));

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {

            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "real", "float", "binary_float", "binary_double", "double" ->
                        scientificNotation(columnName);
                default ->
                        Props.getProperty("number-cast").equals("notation") ? scientificNotation(columnName) : "nvl(trim(to_char(" + columnName+ ", '" + Props.getProperty("standard-number-format") + "')),' ')";
            };


        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "nvl(to_char(" + columnName + "),'0')";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("time zone") || column.getString("dataType").toLowerCase().contains("tz") ) {
                colExpression = "nvl(to_char(" + columnName + " at time zone 'UTC','MMDDYYYYHH24MISS'),' ')";
            } else {
                colExpression = "nvl(to_char(" + columnName + ",'MMDDYYYYHH24MISS'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = column.getInt("dataLength") > 1 ? "case when length(" + columnName + ")=0 then ' ' else coalesce(trim(" + columnName + "),' ') end" :  "case when length(" + columnName + ")=0 then ' ' else " + columnName + " end";
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when dbms_lob.getlength(" + columnName +") = 0 or " + columnName + " is null then ' ' else lower(dbms_crypto.hash(" + columnName + ",2)) end";
        } else {
            colExpression = columnName;
        }

        return colExpression;

    }

    /**
     * Establishes a connection to an DB2 database using the provided connection properties.
     *
     * @param connectionProperties Properties containing database connection information.
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to DB2 database.
     */
    public static Connection getConnection(Properties connectionProperties, String destType) {
        Connection conn = null;
        String url = "jdbc:db2://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));

        try {
            conn = DriverManager.getConnection(url,dbProps);
            conn.setAutoCommit(true);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error connecting to DB2 %s",e.getMessage()));
        }

        return conn;

    }

    /**
     * DB2 does not support controlling the format of scientific notation.  Therefore,
     * this routine will construct a column expression for SQL that will return a formatted
     * scientific notation that matches other platforms.
     *
     * @param columnName Column name.
     * @return String object with column expression.
     */
    public static String scientificNotation(String columnName) {

        String sqlFunction = String.format("CASE WHEN %s = 0 THEN '0.000000e+00' ELSE (CASE WHEN %s < 0 THEN '-' ELSE '' END) || substr(trim(char(CAST(round(abs(%s)/pow(10,floor(log10(abs(%s)))),6) AS float))),1,instr(trim(char(CAST(round(abs(%s)/pow(10,floor(log10(abs(%s)))),6) AS float))),'E')-1) || 'e' || (CASE WHEN floor(log10(abs(%s))) >= 0 THEN '+' ELSE '-' END) || lpad(trim(char(CAST(floor(log10(abs(%s))) AS integer))),2,'0') END",columnName,columnName,columnName,columnName,columnName,columnName,columnName,columnName);

        return sqlFunction;
    }

}
