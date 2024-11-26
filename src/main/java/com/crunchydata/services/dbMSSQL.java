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
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import static com.crunchydata.util.ColumnUtility.*;
import static com.crunchydata.util.DataUtility.ShouldQuoteString;
import static com.crunchydata.util.DataUtility.preserveCase;
import static com.crunchydata.util.SQLConstantsMSSQL.SQL_MSSQL_SELECT_COLUMNS;
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
     * @return SQL query string for loading data from the specified table.
     */
    public static String buildLoadSQL (Boolean useDatabaseHash, DCTableMap tableMap, ColumnMetadata columnMetadata) {
        String sql = "SELECT ";

        if (useDatabaseHash) {
            sql += "lower(convert(varchar, hashbytes('MD5'," +  columnMetadata.getPk() + "),2)) pk_hash, " + columnMetadata.getPkJSON() + " pk, lower(convert(varchar, hashbytes('MD5'," + columnMetadata.getColumn() + "),2)) column_hash FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        } else {
            sql +=  columnMetadata.getPk() + " pk_hash, " + columnMetadata.getPkJSON() + " pk, " + columnMetadata.getColumn() + " FROM " + ShouldQuoteString(tableMap.isSchemaPreserveCase(), tableMap.getSchemaName()) + "." + ShouldQuoteString(tableMap.isTablePreserveCase(),tableMap.getTableName()) + " WHERE 1=1 ";
        }

        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().isEmpty()) {
            sql += " AND " + tableMap.getTableFilter();
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
        String columnName = ShouldQuoteString(column.getBoolean("preserveCase"), column.getString("columnName"));

        if ( Arrays.asList(numericTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "real", "float", "float4", "float8" ->
                        "lower(replace(coalesce(trim(format(" + columnName + ",'E6')),' '),'E+0','e+'))";
                default ->
                        Props.getProperty("number-cast").equals("notation") ? "lower(replace(coalesce(trim(format(" + columnName + ",'E10')),' '),'E+0','e+'))"   : "coalesce(cast(format(" + columnName + ", '"+ Props.getProperty("standard-number-format") + "') as text),' ')";
            };
        } else if ( Arrays.asList(booleanTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = "case when coalesce(cast(" + columnName + " as varchar),'0') = 'true' then '1' else '0' end";
        } else if ( Arrays.asList(timestampTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = switch (column.getString("dataType").toLowerCase()) {
                case "date" ->
                        "coalesce(format(" + columnName + ",'MMddyyyyHHmmss'),' ')";
                default ->
                        "coalesce(format(" + columnName + " at time zone 'UTC','MMddyyyyHHmmss'),' ')";
            };
        } else if ( Arrays.asList(charTypes).contains(column.getString("dataType").toLowerCase()) ) {
            colExpression = switch (column.getString("dataType").toLowerCase()) {
                // Cannot use trim on text data type.
                case "text" ->
                        "coalesce(" + columnName + ",' ')";
                default ->
                        column.getInt("dataLength") > 1 ? "case when len(" + columnName + ")=0 then ' ' else coalesce(rtrim(ltrim(" + columnName + ")),' ') end" :  "case when len(" + columnName + ")=0 then ' ' else " + columnName + " end";
            };
        } else {
            colExpression = columnName;
        }

        return colExpression;

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
