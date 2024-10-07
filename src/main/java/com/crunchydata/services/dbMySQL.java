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

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DCTableMap;
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
import static com.crunchydata.util.DataUtility.ShouldQuoteString;
import static com.crunchydata.util.DataUtility.preserveCase;
import static com.crunchydata.util.SQLConstantsMYSQL.SQL_MYSQL_SELECT_COLUMNS;
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
    public static final String nativeCase = "lower";

    private static final String THREAD_NAME = "dbMySQL";

    /**
     * Builds a SQL query for loading data from a MySQL table.
     *
     * @param useDatabaseHash Whether to use MD5 hash for database columns.
     * @return SQL query string for loading data from the specified table.
     */
    public static String buildLoadSQL (Boolean useDatabaseHash, DCTableMap tableMap, ColumnMetadata columnMetadata) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "lower(md5(" +  columnMetadata.getPk() + ")) pk_hash, " + columnMetadata.getPkJSON() + " pk, lower(md5(" + columnMetadata.getColumn() + ")) column_hash FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        } else {
            sql +=  columnMetadata.getPk() + " pk_hash, " + columnMetadata.getPkJSON() + " pk, " + columnMetadata.getColumn() + " FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        }

        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().isEmpty()) {
            sql += " AND " + tableMap.getTableFilter();
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
        String columnName = ShouldQuoteString(column.getBoolean("preserveCase"), column.getString("columnName"));

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            switch (column.getString("dataType").toLowerCase()) {
                case "float":
                case "double":
                    colExpression = "coalesce(if(" + columnName + "=0,'0.000000e+00',concat(if(" + columnName + "<0, '-', ''),format(abs(" + columnName + ")/pow(10, floor(log10(abs(" + columnName + ")))), 6),'e',if(floor(log10(abs(" + columnName + ")))>=0,'+','-'),lpad(replace(replace(convert(FORMAT(floor(log10(abs(" + columnName + "))), 2)/100,char),'0.',''),'-',''),2,'0'))),' ')";
                    break;
                default:
                    if (Props.getProperty("number-cast").equals("notation")) {
                        colExpression = "coalesce(if(" + columnName + "=0,'0.0000000000e+00',concat(if(" + columnName + "<0, '-', ''),format(abs(" + columnName + ")/pow(10, floor(log10(abs(" + columnName + ")))), 10),'e',if(floor(log10(abs(" + columnName + ")))>=0,'+','-'),lpad(replace(replace(convert(FORMAT(floor(log10(abs(" + columnName + "))), 2)/100,char),'0.',''),'-',''),2,'0'))),' ')";
                    } else {
                        colExpression = "coalesce(if(instr(convert(" + columnName + ",char),'.')>0,concat(if(" + columnName + "<0,'-',''),lpad(substring_index(convert(abs(" + columnName + "),char),'.',1),22,'0'),'.',rpad(substring_index(convert(" + columnName + ",char),'.',-1),22,'0')),concat(if(" + columnName + "<0,'-',''),lpad(convert(" + columnName + ",char),22,'0'),'.',rpad('',22,'0'))),' ')";
                    }
            }
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(convert(" + columnName + ",char),'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            if (column.getString("dataType").toLowerCase().contains("timestamp") || column.getString("dataType").toLowerCase().contains("datetime") ) {
                colExpression = "coalesce(date_format(convert_tz(" + columnName + ",@@session.time_zone,'UTC'),'%m%d%Y%H%i%S'),' ')";
            } else {
                colExpression = "coalesce(date_format(" + columnName + ",'%m%d%Y%H%i%S'),' ')";
            }
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = column.getInt("dataLength") > 1 ? "case when length(" + columnName + ")=0 then ' ' else coalesce(trim(" + columnName + "),' ') end" :  "case when length(" + columnName + ")=0 then ' ' else " + columnName + " end";
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(md5(" + columnName +"), ' ')";
        } else {
            colExpression = columnName;
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
                column.put("dataClass", getDataClass(rs.getString("data_type").toLowerCase()));
                column.put("preserveCase",preserveCase(nativeCase,rs.getString("column_name")));
                column.put("valueExpression", columnValueMapMySQL(column));

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
