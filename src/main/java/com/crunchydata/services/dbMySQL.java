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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.util.ColumnUtility.*;
import static com.crunchydata.util.DataUtility.ShouldQuoteString;

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
    public static final String quoteChar = "`";

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
            sql += "lower(md5(" +  columnMetadata.getPk() + ")) pk_hash, " + columnMetadata.getPkJSON() + " pk, lower(md5(" + columnMetadata.getColumn() + ")) column_hash FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName(), quoteChar) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName(), quoteChar) + " WHERE 1=1 ";
        } else {
            sql +=  columnMetadata.getPk() + " pk_hash, " + columnMetadata.getPkJSON() + " pk, " + columnMetadata.getColumn() + " FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName(), quoteChar) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName(), quoteChar) + " WHERE 1=1 ";
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
    public static String columnValueMapMySQL(Properties Props, JSONObject column) {
        String colExpression;
        String columnName = ShouldQuoteString(column.getBoolean("preserveCase"), column.getString("columnName"), quoteChar);

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
            colExpression = column.getInt("dataLength") > 1 ? "case when length(" + columnName + ")=0 then ' ' else coalesce(trim(" + columnName + "),' ') end" :  "case when length(" + columnName + ")=0 then ' ' else trim(" + columnName + ") end";
        } else if ( Arrays.asList(binaryTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "coalesce(md5(" + columnName +"), ' ')";
        } else {
            colExpression = "case when length(" + columnName + ")=0 then ' ' else " + columnName + " end";
        }

        return colExpression;

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
