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

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class that contains common actions performed against the database
 * which are agnostic to the database platform. Provides comprehensive SQL
 * execution capabilities with proper error handling, resource management,
 * and performance optimizations.
 *
 * @author Brian Pace
 */
public class SQLExecutionService {
    private static final String THREAD_NAME = "sql-service";
    
    // Performance and configuration constants
    private static final int DEFAULT_FETCH_SIZE = 2000;

    // Query execution result wrapper
        public record QueryResult<T>(T result, boolean success, String errorMessage, long executionTimeMs) {
    }

    /**
     * Executes a parameterized SQL query and returns the results as a CachedRowSet.
     * This method provides comprehensive error handling, resource management, and performance optimization.
     *
     * @param conn The database Connection object to use for executing the query
     * @param sql The SQL query to execute, with placeholders for parameters
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement
     * @return A CachedRowSet containing the results of the query, null if execution fails
     * @throws IllegalArgumentException if required parameters are null
     */
    public static CachedRowSet simpleSelect(Connection conn, String sql, ArrayList<Object> binds) {
        // Input validation
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(sql, "SQL cannot be null");
        Objects.requireNonNull(binds, "Binds cannot be null");
        
        if (sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL cannot be empty");
        }
        
        long startTime = System.currentTimeMillis();
        CachedRowSet crs = null;
        
        try {
            // Create a new CachedRowSet
            crs = RowSetProvider.newFactory().createCachedRowSet();
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                // Set fetch size for performance optimization
                stmt.setFetchSize(DEFAULT_FETCH_SIZE);
                
                // Bind parameters to the PreparedStatement
                bindParameters(stmt, binds);
                
                // Execute the query and populate the ResultSet
                try (ResultSet rs = stmt.executeQuery()) {
                    crs.populate(rs);
                }
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("debug", THREAD_NAME,
                String.format("Query executed successfully in %dms: %s", executionTime, sql));
                
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        }
        
        return crs;
    }
    
    /**
     * Enhanced version of simpleSelect with detailed result information.
     *
     * @param conn The database Connection object
     * @param sql The SQL query to execute
     * @param binds The parameters to bind
     * @return QueryResult containing the CachedRowSet and execution details
     */
    public static QueryResult<CachedRowSet> simpleSelectWithResult(Connection conn, String sql, ArrayList<Object> binds) {
        // Input validation
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(sql, "SQL cannot be null");
        Objects.requireNonNull(binds, "Binds cannot be null");
        
        if (sql.trim().isEmpty()) {
            return new QueryResult<>(null, false, "SQL cannot be empty", 0);
        }
        
        long startTime = System.currentTimeMillis();
        CachedRowSet crs = null;
        String errorMessage = null;
        
        try {
            crs = RowSetProvider.newFactory().createCachedRowSet();
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setFetchSize(DEFAULT_FETCH_SIZE);
                bindParameters(stmt, binds);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    crs.populate(rs);
                }
            }
            
        } catch (SQLException e) {
            errorMessage = String.format("SQL error: %s", e.getMessage());
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing query: %s - %s", sql, e.getMessage()));
        } catch (Exception e) {
            errorMessage = String.format("Unexpected error: %s", e.getMessage());
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing query: %s - %s", sql, e.getMessage()));
        }
        
        long executionTime = System.currentTimeMillis() - startTime;
        boolean success = (crs != null && errorMessage == null);
        
        return new QueryResult<>(crs, success, errorMessage, executionTime);
    }
    
    /**
     * Helper method to bind parameters to a PreparedStatement.
     */
    private static void bindParameters(PreparedStatement stmt, ArrayList<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            stmt.setObject(i + 1, binds.get(i));
        }
    }
    
    /**
     * Validates connection and SQL parameters.
     */
    private static void validateParameters(Connection conn, String sql, ArrayList<Object> binds) {
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(sql, "SQL cannot be null");
        Objects.requireNonNull(binds, "Binds cannot be null");
        
        if (sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL cannot be empty");
        }
    }

    /**
     * Executes a parameterized SQL query and returns a single Integer value.
     *
     * <p>This method prepares a PreparedStatement with the provided SQL query and binds parameters
     * from the given ArrayList. It then executes the query, extracts the first row and first
     * column and returns the value.</p>
     *
     * <p>If any exception occurs during the execution, a severe-level log message is written
     * using the Logging utility.</p>
     *
     * @param conn The database Connection object to use for executing the query.
     * @param sql The SQL query to execute, with placeholders for parameters.
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement.
     * @return Integer.
     */
    public static Integer simpleSelectReturnInteger(Connection conn, String sql, ArrayList<Object> binds) {
        validateParameters(conn, sql, binds);
        
        long startTime = System.currentTimeMillis();
        Integer returnValue = null;
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            bindParameters(stmt, binds);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    returnValue = rs.getInt(1);
                }
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("debug", THREAD_NAME,
                String.format("Integer query executed successfully in %dms: %s", executionTime, sql));
                
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing integer query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing integer query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        }
        
        return returnValue;
    }

    /**
     * Executes a parameterized SQL query and returns a single String value.
     * This method provides comprehensive error handling and resource management.
     *
     * @param conn The database Connection object to use for executing the query
     * @param sql The SQL query to execute, with placeholders for parameters
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement
     * @return String value from the first row and first column, null if no results or error occurs
     * @throws IllegalArgumentException if required parameters are null
     */
    public static String simpleSelectReturnString(Connection conn, String sql, ArrayList<Object> binds) {
        validateParameters(conn, sql, binds);
        
        long startTime = System.currentTimeMillis();
        String returnValue = null;
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            bindParameters(stmt, binds);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    returnValue = rs.getString(1);
                }
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("debug", THREAD_NAME,
                String.format("String query executed successfully in %dms: %s", executionTime, sql));
                
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing string query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing string query (%dms): %s - %s", executionTime, sql, e.getMessage()));
        }
        
        return returnValue;
    }

    /**
     * Executes a parameterized DML SQL query and returns the count of rows impacted.
     * This method provides comprehensive error handling, transaction management, and performance optimization.
     *
     * @param conn The database Connection object to use for executing the query
     * @param sql The SQL query to execute, with placeholders for parameters
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement
     * @param commit Whether to commit the transaction after execution
     * @return Integer containing the number of rows impacted by the query, -1 if error occurs
     * @throws IllegalArgumentException if required parameters are null
     */
    public static Integer simpleUpdate(Connection conn, String sql, ArrayList<Object> binds, Boolean commit) {
        validateParameters(conn, sql, binds);
        Objects.requireNonNull(commit, "Commit parameter cannot be null");
        
        long startTime = System.currentTimeMillis();
        int cnt = -1;
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            bindParameters(stmt, binds);
            
            // Execute the query and get impacted row count
            cnt = stmt.executeUpdate();
            
            // Conditionally commit transaction
            if (commit) {
                conn.commit();
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("debug", THREAD_NAME,
                String.format("Update executed successfully in %dms, affected %d rows: %s", executionTime, cnt, sql));
                
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing update (%dms): %s - %s", executionTime, sql, e.getMessage()));
            
            try {
                conn.rollback();
            } catch (SQLException rollbackException) {
                LoggingUtils.write("warning", THREAD_NAME,
                    String.format("Failed to rollback transaction: %s", rollbackException.getMessage()));
            }
            
            cnt = -1;
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing update (%dms): %s - %s", executionTime, sql, e.getMessage()));
            
            try {
                conn.rollback();
            } catch (SQLException rollbackException) {
                LoggingUtils.write("warning", THREAD_NAME,
                    String.format("Failed to rollback transaction: %s", rollbackException.getMessage()));
            }
            
            cnt = -1;
        }
        
        return cnt;
    }
    
    /**
     * Executes a batch of SQL statements for improved performance.
     *
     * @param conn The database Connection object
     * @param sqlStatements List of SQL statements to execute
     * @param commit Whether to commit after all statements
     * @return Array of update counts for each statement
     */
    public static int[] executeBatch(Connection conn, List<String> sqlStatements, boolean commit) {
        Objects.requireNonNull(conn, "Connection cannot be null");
        Objects.requireNonNull(sqlStatements, "SQL statements cannot be null");
        
        if (sqlStatements.isEmpty()) {
            throw new IllegalArgumentException("SQL statements list cannot be empty");
        }
        
        long startTime = System.currentTimeMillis();
        int[] updateCounts = new int[0];
        
        try (PreparedStatement stmt = conn.prepareStatement(sqlStatements.get(0))) {
            stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            
            // Add all statements to batch
            for (String sql : sqlStatements) {
                stmt.addBatch(sql);
            }
            
            // Execute batch
            updateCounts = stmt.executeBatch();
            
            if (commit) {
                conn.commit();
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("debug", THREAD_NAME,
                String.format("Batch executed successfully in %dms, %d statements", executionTime, sqlStatements.size()));
                
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("SQL error executing batch (%dms): %s", executionTime, e.getMessage()));
            
            try {
                conn.rollback();
            } catch (SQLException rollbackException) {
                LoggingUtils.write("warning", THREAD_NAME,
                    String.format("Failed to rollback batch transaction: %s", rollbackException.getMessage()));
            }
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error executing batch (%dms): %s", executionTime, e.getMessage()));
        }
        
        return updateCounts;
    }

    /**
     * Utility method to execute a parameterized SQL query (DML) and return the results as a CachedRowSet.
     *
     * <p>This method prepares a PreparedStatement with the provided SQL query and binds parameters
     * from the given ArrayList. It then executes the query, populates a CachedRowSet with the
     * ResultSet data, and returns the CachedRowSet.</p>
     *
     * <p>If any exception occurs during the execution, a severe-level log message is written
     * using the Logging utility.</p>
     *
     * @param conn The database Connection object to use for executing the query.
     * @param sql The SQL query to execute, with placeholders for parameters.
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement.
     * @return A CachedRowSet containing the results of the query.
     */
    public static CachedRowSet simpleUpdateReturning(Connection conn, String sql, ArrayList<Object> binds) {
        CachedRowSet crs = null;

        try {
            // Create a new CachedRowSet
            crs = RowSetProvider.newFactory().createCachedRowSet();

            // Prepare the PreparedStatement with the provided SQL query
            PreparedStatement stmt = conn.prepareStatement(sql);

            stmt.setFetchSize(2000);

            // Bind parameters to the PreparedStatement
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }

            // Execute the query and populate the ResultSet
            ResultSet rs = stmt.executeQuery();
            crs.populate(rs);

            // Close the ResultSet and PreparedStatement
            rs.close();
            stmt.close();
            conn.commit();

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, "Error executing simple update with returning (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
        }

        return crs;
    }

    /**
     * Utility method to execute a parameterized SQL query (DML) and return a single Integer value.
     *
     * <p>This method prepares a PreparedStatement with the provided SQL query and binds parameters
     * from the given ArrayList. It then executes the query, returns a single Integer value.</p>
     *
     * <p>If any exception occurs during the execution, a severe-level log message is written
     * using the Logging utility.</p>
     *
     * @param conn The database Connection object to use for executing the query.
     * @param sql The SQL query to execute, with placeholders for parameters.
     * @param binds The ArrayList containing the parameters to bind to the PreparedStatement.
     * @return Integer.
     */
    public static Integer simpleUpdateReturningInteger(Connection conn, String sql, ArrayList<Object> binds) {
        Integer returnValue = null;

        try {
            // Prepare the PreparedStatement with the provided SQL query
            PreparedStatement stmt = conn.prepareStatement(sql);

            // Bind parameters to the PreparedStatement
            for (int counter = 0; counter < binds.size(); counter++) {
                stmt.setObject(counter+1, binds.get(counter));
            }

            // Execute the query and populate the ResultSet
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                returnValue = rs.getInt(1);
            }

            // Close the ResultSet and PreparedStatement
            rs.close();
            stmt.close();
            conn.commit();

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, "Error executing simple update with returning (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
        }

        return returnValue;
    }

    /**
     * Utility method to execute a parameterized SQL query.
     *
     * <p>This method prepares a PreparedStatement with the provided SQL query (DML) and binds parameters
     * from the given ArrayList. It then executes the query, populates cnt variable with the
     * number of rows impacted.</p>
     *
     * <p>If any exception occurs during the execution, a severe-level log message is written
     * using the Logging utility.</p>
     *
     * @param conn The database Connection object to use for executing the query.
     * @param sql The SQL query to execute, with placeholders for parameters.
     */
    public static void simpleExecute(Connection conn, String sql) {
        try {

            // Prepare the PreparedStatement with the provided SQL query
            PreparedStatement stmt = conn.prepareStatement(sql);

            // Execute the query
            stmt.execute();

            // Close the PreparedStatement
            stmt.close();

            if ( ! conn.getAutoCommit() ) {
                conn.commit();
            }
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, "Error executing simple execute (" + sql + "):  " + e.getMessage());
            try { conn.rollback(); } catch (Exception ee) {
                // do nothing
            }
        }
    }
}
