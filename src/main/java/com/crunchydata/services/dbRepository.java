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

package com.crunchydata.services;

import com.crunchydata.util.Logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.crunchydata.util.SQLConstantsRepo.*;

/**
 * Utility class for creating repository schema, tables, indexes, and constraints.
 * This class provides methods to execute SQL DDL statements for repository setup
 * with comprehensive error handling, transaction management, and validation.
 *
 * @author Brian Pace
 */
public class dbRepository {

    private static final String THREAD_NAME = "db-repository";
    
    // Repository configuration constants
    private static final String REPO_SCHEMA_PROPERTY = "repo-schema";
    private static final String REPO_USER_PROPERTY = "repo-user";
    private static final String DEFAULT_SCHEMA = "pgcompare";
    private static final String DEFAULT_USER = "postgres";
    
    // DDL execution phases
    private enum DDLPhase {
        SCHEMA_CREATION,
        TABLE_CREATION,
        INDEX_CREATION,
        DATA_INSERTION,
        FUNCTION_CREATION
    }

    /**
     * Creates the repository schema, tables, indexes, and constraints in the specified database connection.
     * This method performs a complete repository setup with transaction management and error handling.
     *
     * @param props Configuration properties containing repository settings
     * @param conn The database connection to use for executing SQL statements
     * @throws IllegalArgumentException if required parameters are null or invalid
     * @throws SQLException if database operations fail
     */
    public static void createRepository(Properties props, Connection conn) throws SQLException {
        // Input validation
        Objects.requireNonNull(props, "Properties cannot be null");
        Objects.requireNonNull(conn, "Connection cannot be null");
        
        if (!dbConnection.isConnectionValid(conn)) {
            throw new IllegalArgumentException("Invalid database connection");
        }
        
        Logging.write("info", THREAD_NAME, "Starting repository creation process");
        
        try {
            // Disable auto-commit for transaction management
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            
            try {
                // Execute DDL operations in sequence
                createSchema(props, conn);
                createTables(conn);
                createIndexesAndConstraints(conn);
                insertInitialData(conn);
                createFunctions(conn);
                
                // Commit all changes
                conn.commit();
                Logging.write("info", THREAD_NAME, "Repository creation completed successfully");
                
            } catch (SQLException e) {
                // Rollback on error
                conn.rollback();
                Logging.write("severe", THREAD_NAME, 
                    String.format("Repository creation failed, rolling back: %s", e.getMessage()));
                throw e;
            } finally {
                // Restore original auto-commit setting
                conn.setAutoCommit(originalAutoCommit);
            }
            
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Unexpected error during repository creation: %s", e.getMessage()));
            throw new SQLException("Repository creation failed", e);
        }
    }
    
    /**
     * Creates the repository schema with proper authorization.
     */
    private static void createSchema(Properties props, Connection conn) throws SQLException {
        String schemaName = getPropertyWithDefault(props, REPO_SCHEMA_PROPERTY, DEFAULT_SCHEMA);
        String userName = getPropertyWithDefault(props, REPO_USER_PROPERTY, DEFAULT_USER);
        
        String schemaDDL = String.format(REPO_DDL_SCHEMA, schemaName, userName);
        
        Logging.write("info", THREAD_NAME, 
            String.format("Creating schema: %s with owner: %s", schemaName, userName));
        
        executeDDL(conn, schemaDDL, DDLPhase.SCHEMA_CREATION);
    }
    
    /**
     * Creates all repository tables.
     */
    private static void createTables(Connection conn) throws SQLException {
        List<String> tableDDLs = List.of(
            REPO_DDL_DC_PROJECT,
            REPO_DDL_DC_RESULT,
            REPO_DDL_DC_SOURCE,
            REPO_DDL_DC_TABLE,
            REPO_DDL_DC_TABLE_COLUMN,
            REPO_DDL_DC_TABLE_COLUMN_MAP,
            REPO_DDL_DC_TABLE_HISTORY,
            REPO_DDL_DC_TABLE_MAP,
            REPO_DDL_DC_TARGET
        );
        
        Logging.write("info", THREAD_NAME, "Creating repository tables");
        
        for (String ddl : tableDDLs) {
            executeDDL(conn, ddl, DDLPhase.TABLE_CREATION);
        }
    }
    
    /**
     * Creates indexes and foreign key constraints.
     */
    private static void createIndexesAndConstraints(Connection conn) throws SQLException {
        List<String> indexConstraintDDLs = List.of(
            REPO_DDL_DC_RESULT_IDX1,
            REPO_DDL_DC_TABLE_HISTORY_IDX1,
            REPO_DDL_DC_TABLE_IDX1,
            REPO_DDL_DC_TABLE_COLUMN_IDX1,
            REPO_DDL_DC_TABLE_COLUMN_FK,
            REPO_DDL_DC_TABLE_MAP_FK,
            REPO_DDL_DC_TABLE_COLUMN_MAP_FK
        );
        
        Logging.write("info", THREAD_NAME, "Creating indexes and constraints");
        
        for (String ddl : indexConstraintDDLs) {
            executeDDL(conn, ddl, DDLPhase.INDEX_CREATION);
        }
    }
    
    /**
     * Inserts initial data into the repository.
     */
    private static void insertInitialData(Connection conn) throws SQLException {
        Logging.write("info", THREAD_NAME, "Inserting initial data");
        executeDDL(conn, REPO_DDL_DC_PROJECT_DATA, DDLPhase.DATA_INSERTION);
    }
    
    /**
     * Creates repository functions.
     */
    private static void createFunctions(Connection conn) throws SQLException {
        Logging.write("info", THREAD_NAME, "Creating repository functions");
        executeDDL(conn, REPO_DDL_DC_COPY_TABLE, DDLPhase.FUNCTION_CREATION);
    }
    
    /**
     * Executes a DDL statement with proper error handling and logging.
     */
    private static void executeDDL(Connection conn, String ddl, DDLPhase phase) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        int result = SQLService.simpleUpdate(conn, ddl, binds, true);
        
        if (result < 0) {
            String errorMsg = String.format("DDL execution failed in phase %s", phase);
            Logging.write("severe", THREAD_NAME, errorMsg);
            throw new SQLException(errorMsg);
        }
        
        Logging.write("debug", THREAD_NAME, 
            String.format("DDL executed successfully in phase %s, affected rows: %d", phase, result));
    }
    
    /**
     * Gets a property value with a default fallback.
     */
    private static String getPropertyWithDefault(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }
    
    /**
     * Validates that the repository schema and tables exist and are properly configured.
     *
     * @param conn Database connection to validate against
     * @return true if repository is valid, false otherwise
     */
    public static boolean validateRepository(Connection conn) {
        if (!dbConnection.isConnectionValid(conn)) {
            Logging.write("warning", THREAD_NAME, "Invalid database connection");
            return false;
        }
        
        try {
            // Check if schema exists
            if (!schemaExists(conn)) {
                Logging.write("warning", THREAD_NAME, "Repository schema does not exist");
                return false;
            }
            
            // Check if required tables exist
            if (!requiredTablesExist(conn)) {
                Logging.write("warning", THREAD_NAME, "Required repository tables are missing");
                return false;
            }
            
            Logging.write("info", THREAD_NAME, "Repository validation successful");
            return true;
            
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Repository validation failed: %s", e.getMessage()));
            return false;
        }
    }
    
    /**
     * Checks if the repository schema exists.
     */
    private static boolean schemaExists(Connection conn) throws SQLException {
        String query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pgcompare'";
        ArrayList<Object> binds = new ArrayList<>();
        
        var crs = SQLService.simpleSelect(conn, query, binds);
        return crs != null && crs.next();
    }
    
    /**
     * Checks if all required repository tables exist.
     */
    private static boolean requiredTablesExist(Connection conn) throws SQLException {
        String[] requiredTables = {
            "dc_project", "dc_result", "dc_source", "dc_table", 
            "dc_table_column", "dc_table_column_map", "dc_table_history", 
            "dc_table_map", "dc_target"
        };
        
        for (String tableName : requiredTables) {
            if (!tableExists(conn, tableName)) {
                Logging.write("warning", THREAD_NAME, 
                    String.format("Required table '%s' does not exist", tableName));
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Checks if a specific table exists in the repository schema.
     */
    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        String query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'pgcompare' AND table_name = ?
            """;
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tableName);
        
        var crs = SQLService.simpleSelect(conn, query, binds);
        return crs != null && crs.next();
    }
    
    /**
     * Drops the entire repository schema and all its contents.
     * Use with extreme caution as this will permanently delete all repository data.
     *
     * @param conn Database connection
     * @param schemaName Name of the schema to drop
     * @throws SQLException if drop operation fails
     */
    public static void dropRepository(Connection conn, String schemaName) throws SQLException {
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(schemaName, "Schema name cannot be null");
        
        if (!dbConnection.isConnectionValid(conn)) {
            throw new IllegalArgumentException("Invalid database connection");
        }
        
        Logging.write("warning", THREAD_NAME, 
            String.format("Dropping repository schema: %s", schemaName));
        
        try {
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            
            try {
                String dropSchemaDDL = String.format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName);
                executeDDL(conn, dropSchemaDDL, DDLPhase.SCHEMA_CREATION);
                
                conn.commit();
                Logging.write("info", THREAD_NAME, "Repository dropped successfully");
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
            
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Failed to drop repository: %s", e.getMessage()));
            throw new SQLException("Repository drop failed", e);
        }
    }
    
    /**
     * Gets repository statistics including table counts and sizes.
     *
     * @param conn Database connection
     * @return String containing repository statistics
     */
    public static String getRepositoryStats(Connection conn) {
        if (!dbConnection.isConnectionValid(conn)) {
            return "Invalid database connection";
        }
        
        try {
            StringBuilder stats = new StringBuilder();
            stats.append("Repository Statistics:\n");
            
            // Get table counts
            String[] tables = {"dc_project", "dc_result", "dc_source", "dc_table", 
                             "dc_table_column", "dc_table_column_map", "dc_table_history", 
                             "dc_table_map", "dc_target"};
            
            for (String table : tables) {
                String countQuery = String.format("SELECT COUNT(*) FROM pgcompare.%s", table);
                ArrayList<Object> binds = new ArrayList<>();
                
                var crs = SQLService.simpleSelect(conn, countQuery, binds);
                if (crs != null && crs.next()) {
                    int count = crs.getInt(1);
                    stats.append(String.format("  %s: %d rows\n", table, count));
                }
            }
            
            return stats.toString();
            
        } catch (SQLException e) {
            return String.format("Error getting repository stats: %s", e.getMessage());
        }
    }
    
    /**
     * Creates a backup of the repository by copying all data to backup tables.
     *
     * @param conn Database connection
     * @param backupSuffix Suffix to append to backup table names
     * @throws SQLException if backup operation fails
     */
    public static void backupRepository(Connection conn, String backupSuffix) throws SQLException {
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(backupSuffix, "Backup suffix cannot be null");
        
        if (!dbConnection.isConnectionValid(conn)) {
            throw new IllegalArgumentException("Invalid database connection");
        }
        
        Logging.write("info", THREAD_NAME, 
            String.format("Creating repository backup with suffix: %s", backupSuffix));
        
        try {
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            
            try {
                // Create backup tables and copy data
                String[] tables = {"dc_project", "dc_result", "dc_source", "dc_table", 
                                 "dc_table_column", "dc_table_column_map", "dc_table_history", 
                                 "dc_table_map", "dc_target"};
                
                for (String table : tables) {
                    String backupTable = table + "_" + backupSuffix;
                    String createBackupDDL = String.format(
                        "CREATE TABLE %s AS SELECT * FROM pgcompare.%s", backupTable, table);
                    executeDDL(conn, createBackupDDL, DDLPhase.TABLE_CREATION);
                }
                
                conn.commit();
                Logging.write("info", THREAD_NAME, "Repository backup completed successfully");
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
            
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Repository backup failed: %s", e.getMessage()));
            throw new SQLException("Repository backup failed", e);
        }
    }
}