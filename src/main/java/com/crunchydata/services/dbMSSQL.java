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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

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
    public static final String quoteChar = "\"";
    public static final String columnHash= "lower(convert(varchar, hashbytes('MD5',%s),2)) AS %s";

    private static final String THREAD_NAME = "dbMSSQL";

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
