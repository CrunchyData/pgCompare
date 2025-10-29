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

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.crunchydata.util.DataProcessingUtils.ShouldQuoteString;
import static com.crunchydata.config.Settings.Props;

/**
 * Service class for database operations and platform-specific SQL generation.
 * Provides utilities for building SQL queries, retrieving table information,
 * and executing database operations across different database platforms.
 * 
 * @author Brian Pace
 */
public class DatabaseMetadataService {

    private static final String THREAD_NAME = "database-metadata";
    
    // SQL query constants
    private static final String SELECT_CLAUSE = "SELECT ";
    private static final String FROM_CLAUSE = " FROM ";
    private static final String WHERE_CLAUSE = " WHERE 1=1";
    private static final String AND_CLAUSE = " AND ";
    
    // Column hash method constants
    private static final String HASH_METHOD_RAW = "raw";
    private static final String HASH_METHOD_HYBRID = "hybrid";
    
    // JSON field names
    private static final String SCHEMA_NAME_FIELD = "schemaName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String OWNER_FIELD = "owner";
    private static final String TABLE_NAME_COLUMN = "table_name";
    
    /**
     * Enum representing different database platforms and their specific configurations.
     */
    public enum DatabasePlatform {
        DB2("upper", "\"", "LOWER(HASH(%s,'MD5')) AS %s", "||"),
        ORACLE("upper", "\"", "LOWER(STANDARD_HASH(%s,'MD5')) AS %s", "||"),
        MARIADB("lower", "`", "lower(md5(%s)) AS %s", "||"),
        MYSQL("lower", "`", "lower(md5(%s)) AS %s", "||"),
        MSSQL("lower", "\"", "lower(convert(varchar, hashbytes('MD5',%s),2)) AS %s", "+"),
        POSTGRES("lower", "\"", "lower(md5(%s)) AS %s", "||"),
        SNOWFLAKE("upper", "\"", "lower(md5(%s)) AS %s", "||");
        
        private final String nativeCase;
        private final String quoteChar;
        private final String columnHashTemplate;
        private final String concatOperator;
        
        DatabasePlatform(String nativeCase, String quoteChar, String columnHashTemplate, String concatOperator) {
            this.nativeCase = nativeCase;
            this.quoteChar = quoteChar;
            this.columnHashTemplate = columnHashTemplate;
            this.concatOperator = concatOperator;
        }
        
        public String getNativeCase() { return nativeCase; }
        public String getQuoteChar() { return quoteChar; }
        public String getColumnHashTemplate() { return columnHashTemplate; }
        public String getConcatOperator() { return concatOperator; }
        
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
     * Builds a SQL query for retrieving data from source or target.
     * 
     * @param columnHashMethod The database hash method to use (database, hybrid, raw)
     * @param tableMap Metadata information on table
     * @param columnMetadata Metadata on columns
     * @return SQL query string for loading data from the specified table
     * @throws IllegalArgumentException if required parameters are null or invalid
     */
    public static String buildLoadSQL(String columnHashMethod, DataComparisonTableMap tableMap, ColumnMetadata columnMetadata) {
        // Input validation
        Objects.requireNonNull(tableMap, "tableMap cannot be null");
        Objects.requireNonNull(columnMetadata, "columnMetadata cannot be null");
        Objects.requireNonNull(columnHashMethod, "columnHashMethod cannot be null");
        
        if (tableMap.getDestType() == null || tableMap.getDestType().trim().isEmpty()) {
            throw new IllegalArgumentException("tableMap.destType cannot be null or empty");
        }
        
        String platform = Props.getProperty(String.format("%s-type", tableMap.getDestType()));
        DatabasePlatform dbPlatform = DatabasePlatform.fromString(platform);
        
        StringBuilder sql = new StringBuilder(SELECT_CLAUSE);
        
        // Build column selection based on hash method
        switch (columnHashMethod.toLowerCase()) {
            case HASH_METHOD_RAW:
            case HASH_METHOD_HYBRID:
                sql.append(String.format("%s AS pk_hash, %s AS pk, %s ", 
                    columnMetadata.getPkExpressionList(), 
                    columnMetadata.getPkJSON(), 
                    columnMetadata.getColumnExpressionList()));
                break;
            default:
                // Database hash method
                sql.append(String.format(dbPlatform.getColumnHashTemplate(), 
                    columnMetadata.getPkExpressionList(), "pk_hash, "));
                sql.append(String.format("%s as pk,", columnMetadata.getPkJSON()));
                sql.append(String.format(dbPlatform.getColumnHashTemplate(), 
                    columnMetadata.getColumnExpressionList(), "column_hash"));
                break;
        }
        
        // Build FROM clause with proper quoting
        String schemaName = ShouldQuoteString(tableMap.isSchemaPreserveCase(), 
            tableMap.getSchemaName(), dbPlatform.getQuoteChar());
        String tableName = ShouldQuoteString(tableMap.isTablePreserveCase(), 
            tableMap.getTableName(), dbPlatform.getQuoteChar());
        
        sql.append(FROM_CLAUSE).append(schemaName).append(".").append(tableName).append(WHERE_CLAUSE);
        
        // Add table filter if present
        if (tableMap.getTableFilter() != null && !tableMap.getTableFilter().trim().isEmpty()) {
            sql.append(AND_CLAUSE).append(tableMap.getTableFilter());
        }
        
        return sql.toString();
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
