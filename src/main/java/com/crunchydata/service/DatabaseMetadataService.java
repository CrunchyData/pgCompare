/*
 * Copyright 2012-2025 the original author or authors.
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

package com.crunchydata.service;

import com.crunchydata.util.LoggingUtils;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;


/**
 * Service class for database operations and platform-specific SQL generation.
 * Provides utilities for building SQL queries, retrieving table information,
 * and executing database operations across different database platforms.
 * 
 * @author Brian Pace
 */
public class DatabaseMetadataService {

    private static final String THREAD_NAME = "database-metadata";

    // JSON field names
    private static final String SCHEMA_NAME_FIELD = "schemaName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String OWNER_FIELD = "owner";
    private static final String TABLE_NAME_COLUMN = "table_name";

    // Platform-specific constants
    private static final String ORACLE_URL_TEMPLATE = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String MARIADB_URL_TEMPLATE = "jdbc:mariadb://%s:%s/%s";
    private static final String MYSQL_URL_TEMPLATE = "jdbc:mysql://%s:%s/%s";
    private static final String MSSQL_URL_TEMPLATE = "jdbc:sqlserver://%s:%s";
    private static final String DB2_URL_TEMPLATE = "jdbc:db2://%s:%s/%s";
    private static final String POSTGRES_URL_TEMPLATE = "jdbc:postgresql://%s:%s/%s";
    private static final String SNOWFLAKE_URL_TEMPLATE = "jdbc:snowflake://%s%s/?db=%s";

    /**
     * Enum representing different database platforms and their specific configurations.
     */
    public enum DatabasePlatform {
        DB2("db2", DB2_URL_TEMPLATE, true, false, false, "upper",
                "\"", "LOWER(HASH(%s,'MD5')) AS %s", "||", "replace(%s, '\"', '\\\"')"),
        ORACLE("oracle", ORACLE_URL_TEMPLATE, true, false, false, "upper",
                "\"", "LOWER(STANDARD_HASH(%s,'MD5')) AS %s", "||", "replace(%s, '\"', '\\\"')"),
        MARIADB("mariadb", MARIADB_URL_TEMPLATE, false, true, true, "lower",
                "`", "lower(md5(%s)) AS %s", "||", "replace(%s, '\"', '\\\\\"')"),
        MYSQL("mysql", MYSQL_URL_TEMPLATE, false, true, true, "lower",
                "`", "lower(md5(%s)) AS %s", "||", "replace(%s, '\"', '\\\\\"')"),
        MSSQL("mssql", MSSQL_URL_TEMPLATE, false, false, false, "lower",
                "\"", "lower(convert(varchar, hashbytes('MD5',%s),2)) AS %s", "+", "replace(%s, '\"', '\\\"')"),
        POSTGRES("postgres", POSTGRES_URL_TEMPLATE, false, false, true, "lower",
                "\"", "lower(md5(%s)) AS %s", "||", "replace(%s,'\"', '\\\"')"),
        SNOWFLAKE("snowflake", SNOWFLAKE_URL_TEMPLATE, false, false, true, "upper",
                "\"", "lower(md5(%s)) AS %s", "||", "replace(%s, '\"', '\\\\\"')");

        @Getter
        private final String name;
        @Getter
        private final String urlTemplate;
        @Getter
        private final boolean autoCommit;
        private final boolean requiresAnsiMode;
        private final boolean supportsSSL;

        @Getter
        private final String nativeCase;
        @Getter
        private final String quoteChar;
        @Getter
        private final String columnHashTemplate;
        @Getter
        private final String concatOperator;
        @Getter
        private final String replacePKSyntax;
        
        DatabasePlatform(String name, String urlTemplate, boolean autoCommit,
                         boolean requiresAnsiMode, boolean supportsSSL, String nativeCase,
                         String quoteChar, String columnHashTemplate, String concatOperator, String replacePKSyntax) {
            this.name = name;
            this.urlTemplate = urlTemplate;
            this.autoCommit = autoCommit;
            this.requiresAnsiMode = requiresAnsiMode;
            this.supportsSSL = supportsSSL;
            this.nativeCase = nativeCase;
            this.quoteChar = quoteChar;
            this.columnHashTemplate = columnHashTemplate;
            this.concatOperator = concatOperator;
            this.replacePKSyntax = replacePKSyntax;
        }

        public boolean requiresAnsiMode() { return requiresAnsiMode; }

        /**
         * Get platform configuration by name, with fallback to POSTGRES for unknown platforms.
         */
        public static DatabasePlatform fromString(String platform) {
            if (platform == null || platform.trim().isEmpty()) {
                return POSTGRES;
            }
            
            try {
                return valueOf(platform.toUpperCase());
            } catch (IllegalArgumentException e) {
                LoggingUtils.write("warning", THREAD_NAME,
                    String.format("Unknown platform '%s', defaulting to POSTGRES", platform));
                return POSTGRES;
            }
        }
    }
    
    /**
     * Get the native case handling for the specified platform.
     * @param platform The database platform name
     * @return The native case string ("upper" or "lower")
     */
    public static String getNativeCase(String platform) {
        return DatabasePlatform.fromString(platform).getNativeCase();
    }

    /**
     * Get the quote character for the specified platform.
     * @param platform The database platform name
     * @return The quote character for identifiers
     */
    public static String getQuoteChar(String platform) {
        return DatabasePlatform.fromString(platform).getQuoteChar();
    }

    /**
     * Get the concatenation operator for the specified platform.
     * @param platform The database platform name
     * @return The concatenation operator
     */
    public static String getConcatOperator(String platform) {
        return DatabasePlatform.fromString(platform).getConcatOperator();
    }

    /**
     * Get the replace command syntax for the specified platform.
     * @param platform The database platform name
     * @return The replace command syntax
     */
    public static String getReplacePKSyntax(String platform) {
        return DatabasePlatform.fromString(platform).getReplacePKSyntax();
    }

    /**
     * Utility method to execute a provided SQL query and retrieve a list of tables.
     *
     * @param conn The database Connection object to use for executing the query
     * @param schema The schema owner of the tables
     * @param tableFilter Optional filter for table names
     * @param sql The SQL query to retrieve table information
     * @return A JSONArray of table information, empty array if error occurs
     * @throws IllegalArgumentException if required parameters are null
     */
    public static JSONArray getTables(Connection conn, String schema, String tableFilter, String sql) {
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(schema, "Schema cannot be null");
        Objects.requireNonNull(sql, "SQL cannot be null");
        
        JSONArray tableInfo = new JSONArray();
        
        if (tableFilter == null) {
            tableFilter = "";
        }

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, schema);
            
            if (!tableFilter.trim().isEmpty()) {
                stmt.setObject(2, tableFilter);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    JSONObject table = new JSONObject();
                    table.put(SCHEMA_NAME_FIELD, rs.getString(OWNER_FIELD));
                    table.put(TABLE_NAME_FIELD, rs.getString(TABLE_NAME_COLUMN));
                    tableInfo.put(table);
                }
            }
        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error retrieving tables for schema '%s': %s", schema, e.getMessage()));
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error retrieving tables for schema '%s': %s", schema, e.getMessage()));
        }

        return tableInfo;
    }

}
