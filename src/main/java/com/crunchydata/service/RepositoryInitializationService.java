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

import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Utility class for creating repository schema, tables, indexes, and constraints.
 * This class provides methods to execute SQL DDL statements for repository setup
 * with comprehensive error handling, transaction management, and validation.
 *
 * @author Brian Pace
 */
public class RepositoryInitializationService {

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
        
        if (!DatabaseConnectionService.isConnectionValid(conn)) {
            throw new IllegalArgumentException("Invalid database connection");
        }
        
        LoggingUtils.write("info", THREAD_NAME, "Starting repository creation process");
        
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
                LoggingUtils.write("info", THREAD_NAME, "Repository creation completed successfully");
                
            } catch (SQLException e) {
                // Rollback on error
                conn.rollback();
                LoggingUtils.write("severe", THREAD_NAME,
                    String.format("Repository creation failed, rolling back: %s", e.getMessage()));
                throw e;
            } finally {
                // Restore original auto-commit setting
                conn.setAutoCommit(originalAutoCommit);
            }
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
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
        
        LoggingUtils.write("info", THREAD_NAME,
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
        
        LoggingUtils.write("info", THREAD_NAME, "Creating repository tables");
        
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
        
        LoggingUtils.write("info", THREAD_NAME, "Creating indexes and constraints");
        
        for (String ddl : indexConstraintDDLs) {
            executeDDL(conn, ddl, DDLPhase.INDEX_CREATION);
        }
    }
    
    /**
     * Inserts initial data into the repository.
     */
    private static void insertInitialData(Connection conn) throws SQLException {
        LoggingUtils.write("info", THREAD_NAME, "Inserting initial data");
        executeDDL(conn, REPO_DDL_DC_PROJECT_DATA, DDLPhase.DATA_INSERTION);
    }
    
    /**
     * Creates repository functions.
     */
    private static void createFunctions(Connection conn) throws SQLException {
        LoggingUtils.write("info", THREAD_NAME, "Creating repository functions");
        executeDDL(conn, REPO_DDL_DC_COPY_TABLE, DDLPhase.FUNCTION_CREATION);
    }
    
    /**
     * Executes a DDL statement with proper error handling and logging.
     */
    private static void executeDDL(Connection conn, String ddl, DDLPhase phase) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        int result = SQLExecutionHelper.simpleUpdate(conn, ddl, binds, true);
        
        if (result < 0) {
            String errorMsg = String.format("DDL execution failed in phase %s", phase);
            LoggingUtils.write("severe", THREAD_NAME, errorMsg);
            throw new SQLException(errorMsg);
        }
        
        LoggingUtils.write("debug", THREAD_NAME,
            String.format("DDL executed successfully in phase %s, affected rows: %d", phase, result));
    }
    
    /**
     * Gets a property value with a default fallback.
     */
    private static String getPropertyWithDefault(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }

}