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
import java.util.Properties;

import com.crunchydata.util.Logging;

/**
 * Utility class for interacting with Postgres databases.
 * This class provides methods for database connection, SQL query generation,
 * column information retrieval, and data type mapping.
 *
 * @author Brian Pace
 */
public class dbPostgres {
    public static final String nativeCase = "lower";
    public static final String quoteChar = "\"";
    public static final String columnHash= "lower(md5(%s)) AS %s";

    private static final String THREAD_NAME = "dbPostgres";

    /**
     * Establishes a connection to a Postgres database using the provided connection properties.
     *
     * @param connectionProperties Properties containing database connection information.
     * @param destType             Type of destination (e.g., source, target).
     * @return Connection object to Postgres database.
     */
    public static Connection getConnection(Properties connectionProperties, String destType, String module) {
        Connection conn = null;
        String url = "jdbc:postgresql://"+connectionProperties.getProperty(destType+"-host")+":"+connectionProperties.getProperty(destType+"-port")+"/"+connectionProperties.getProperty(destType+"-dbname")+"?sslmode="+connectionProperties.getProperty(destType+"-sslmode");
        Properties dbProps = new Properties();

        dbProps.setProperty("user",connectionProperties.getProperty(destType+"-user"));
        dbProps.setProperty("password",connectionProperties.getProperty(destType+"-password"));
        dbProps.setProperty("options","-c search_path="+connectionProperties.getProperty(destType+"-schema")+",public,pg_catalog");
        dbProps.setProperty("reWriteBatchedInserts", "true");
        dbProps.setProperty("preparedStatementCacheQueries", "5");
        dbProps.setProperty("ApplicationName", "pgCompare - " + module);
        dbProps.setProperty("synchronous_commit", "off");

        try {
            conn = DriverManager.getConnection(url,dbProps);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error connecting to Postgres:  %s", e.getMessage()));
        }

        return conn;

    }

}
