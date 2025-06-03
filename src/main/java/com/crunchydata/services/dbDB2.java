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
 * Utility class for interacting with DB2 databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 *
 *
 * @author Brian Pace
 */
public class dbDB2 {
    public static final String nativeCase = "upper";
    public static final String quoteChar = "\"";
    public static final String columnHash= "LOWER(HASH(%s,'MD5')) AS %s";

    private static final String THREAD_NAME = "dbDB2";

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

}
