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

import com.crunchydata.util.Logging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

import static com.crunchydata.util.Settings.Props;

/**
 * Service class for managing database connections across different platforms.
 * Provides utilities for establishing, configuring, and managing database connections
 * with platform-specific optimizations and error handling.
 * 
 * @author Brian Pace
 */
public class dbConnection {

    private static final String THREAD_NAME = "connection";
    
    // Connection property constants
    private static final String USER_PROPERTY = "user";
    private static final String PASSWORD_PROPERTY = "password";
    private static final String OPTIONS_PROPERTY = "options";
    private static final String REWRITE_BATCHED_INSERTS = "reWriteBatchedInserts";
    private static final String PREPARED_STATEMENT_CACHE = "preparedStatementCacheQueries";
    private static final String APPLICATION_NAME = "ApplicationName";
    private static final String SYNCHRONOUS_COMMIT = "synchronous_commit";
    
    // Platform-specific constants
    private static final String POSTGRES_APP_NAME = "pgcompare";
    private static final String POSTGRES_SEARCH_PATH_TEMPLATE = "-c search_path=%s,public,pg_catalog";
    private static final String POSTGRES_SSL_MODE_TEMPLATE = "?sslmode=%s";
    private static final String MYSQL_SSL_TEMPLATE = "?allowPublicKeyRetrieval=true&useSSL=%s";
    private static final String MSSQL_ENCRYPT_TEMPLATE = ";databaseName=%s;encrypt=%s";
    private static final String ORACLE_URL_TEMPLATE = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String MARIADB_URL_TEMPLATE = "jdbc:mariadb://%s:%s/%s";
    private static final String MYSQL_URL_TEMPLATE = "jdbc:mysql://%s:%s/%s";
    private static final String MSSQL_URL_TEMPLATE = "jdbc:sqlserver://%s:%s";
    private static final String DB2_URL_TEMPLATE = "jdbc:db2://%s:%s/%s";
    private static final String POSTGRES_URL_TEMPLATE = "jdbc:postgresql://%s:%s/%s";
    
    // SQL mode constants
    private static final String ANSI_SQL_MODE = "set session sql_mode='ANSI'";
    
    /**
     * Enum representing different database platforms and their connection configurations.
     */
    public enum DatabasePlatform {
        ORACLE("oracle", ORACLE_URL_TEMPLATE, true, false, false),
        MARIADB("mariadb", MARIADB_URL_TEMPLATE, false, true, true),
        MYSQL("mysql", MYSQL_URL_TEMPLATE, false, true, true),
        MSSQL("mssql", MSSQL_URL_TEMPLATE, false, false, false),
        DB2("db2", DB2_URL_TEMPLATE, true, false, false),
        POSTGRES("postgres", POSTGRES_URL_TEMPLATE, false, false, true);
        
        private final String name;
        private final String urlTemplate;
        private final boolean autoCommit;
        private final boolean requiresAnsiMode;
        private final boolean supportsSSL;
        
        DatabasePlatform(String name, String urlTemplate, boolean autoCommit, 
                        boolean requiresAnsiMode, boolean supportsSSL) {
            this.name = name;
            this.urlTemplate = urlTemplate;
            this.autoCommit = autoCommit;
            this.requiresAnsiMode = requiresAnsiMode;
            this.supportsSSL = supportsSSL;
        }
        
        public String getName() { return name; }
        public String getUrlTemplate() { return urlTemplate; }
        public boolean isAutoCommit() { return autoCommit; }
        public boolean requiresAnsiMode() { return requiresAnsiMode; }
        public boolean supportsSSL() { return supportsSSL; }
        
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
                Logging.write("warning", THREAD_NAME, 
                    String.format("Unknown platform '%s', defaulting to POSTGRES", platform));
                return POSTGRES;
            }
        }
    }

    /**
     * Safely closes a database connection with proper error handling.
     *
     * @param conn Database connection to close
     */
    public static void closeDatabaseConnection(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                    Logging.write("debug", THREAD_NAME, "Database connection closed successfully");
                }
            } catch (SQLException e) {
                Logging.write("warning", THREAD_NAME, 
                    String.format("Error closing database connection: %s", e.getMessage()));
            } catch (Exception e) {
                Logging.write("warning", THREAD_NAME, 
                    String.format("Unexpected error closing database connection: %s", e.getMessage()));
            }
        }
    }
    
    /**
     * Validates that a database connection is open and valid.
     *
     * @param conn Database connection to validate
     * @return true if connection is valid and open, false otherwise
     */
    public static boolean isConnectionValid(Connection conn) {
        if (conn == null) {
            return false;
        }
        
        try {
            return !conn.isClosed() && conn.isValid(5); // 5 second timeout
        } catch (SQLException e) {
            Logging.write("warning", THREAD_NAME, 
                String.format("Error validating connection: %s", e.getMessage()));
            return false;
        }
    }

    /**
     * Establishes a database connection using platform-specific configuration.
     *
     * @param platform The database platform (oracle, mariadb, mysql, mssql, db2, postgres)
     * @param destType Type of destination (e.g., source, target)
     * @return Connection object to the database, null if connection fails
     * @throws IllegalArgumentException if required parameters are null or invalid
     */
    public static Connection getConnection(String platform, String destType) {
        // Input validation
        Objects.requireNonNull(platform, "Platform cannot be null");
        Objects.requireNonNull(destType, "Destination type cannot be null");
        
        if (platform.trim().isEmpty() || destType.trim().isEmpty()) {
            throw new IllegalArgumentException("Platform and destination type cannot be empty");
        }
        
        DatabasePlatform dbPlatform = DatabasePlatform.fromString(platform);
        
        try {
            // Build connection URL
            String url = buildConnectionUrl(dbPlatform, destType);
            
            // Build connection properties
            Properties dbProps = buildConnectionProperties(dbPlatform, destType);
            
            // Establish connection
            Connection conn = DriverManager.getConnection(url, dbProps);
            
            // Configure platform-specific settings
            configureConnection(conn, dbPlatform);
            
            Logging.write("info", THREAD_NAME, 
                String.format("Successfully connected to %s database (%s)", platform, destType));
            
            return conn;
            
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("SQL error connecting to %s (%s): %s", platform, destType, e.getMessage()));
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Unexpected error connecting to %s (%s): %s", platform, destType, e.getMessage()));
        }
        
        return null;
    }
    
    /**
     * Builds the connection URL for the specified platform and destination.
     */
    private static String buildConnectionUrl(DatabasePlatform platform, String destType) {
        String host = Props.getProperty(destType + "-host");
        String port = Props.getProperty(destType + "-port");
        String dbname = Props.getProperty(destType + "-dbname");
        
        if (host == null || port == null || dbname == null) {
            throw new IllegalArgumentException(
                String.format("Missing required connection properties for %s", destType));
        }
        
        StringBuilder url = new StringBuilder();
        url.append(String.format(platform.getUrlTemplate(), host, port, dbname));
        
        // Add platform-specific URL parameters
        switch (platform) {
            case MARIADB:
            case MYSQL:
                String sslMode = Props.getProperty(destType + "-sslmode");
                boolean useSSL = sslMode != null && !sslMode.equals("disable");
                url.append(String.format(MYSQL_SSL_TEMPLATE, useSSL));
                break;
            case MSSQL:
                String sslModeMssql = Props.getProperty(destType + "-sslmode");
                boolean encryptMssql = sslModeMssql != null && !sslModeMssql.equals("disable");
                url.append(String.format(MSSQL_ENCRYPT_TEMPLATE, dbname, encryptMssql));
                break;
            case POSTGRES:
                String sslModePg = Props.getProperty(destType + "-sslmode");
                if (sslModePg != null) {
                    url.append(String.format(POSTGRES_SSL_MODE_TEMPLATE, sslModePg));
                }
                break;
            case ORACLE:
            case DB2:
                // No additional URL parameters needed for Oracle and DB2
                break;
        }
        
        return url.toString();
    }
    
    /**
     * Builds connection properties for the specified platform and destination.
     */
    private static Properties buildConnectionProperties(DatabasePlatform platform, String destType) {
        Properties props = new Properties();
        
        // Basic authentication
        String user = Props.getProperty(destType + "-user");
        String password = Props.getProperty(destType + "-password");
        
        if (user == null || password == null) {
            throw new IllegalArgumentException(
                String.format("Missing authentication properties for %s", destType));
        }
        
        props.setProperty(USER_PROPERTY, user);
        props.setProperty(PASSWORD_PROPERTY, password);
        
        // Platform-specific properties
        if (platform == DatabasePlatform.POSTGRES) {
            String schema = Props.getProperty(destType + "-schema");
            if (schema != null) {
                props.setProperty(OPTIONS_PROPERTY, 
                    String.format(POSTGRES_SEARCH_PATH_TEMPLATE, schema));
            }
            props.setProperty(REWRITE_BATCHED_INSERTS, "true");
            props.setProperty(PREPARED_STATEMENT_CACHE, "5");
            props.setProperty(APPLICATION_NAME, POSTGRES_APP_NAME);
            props.setProperty(SYNCHRONOUS_COMMIT, "off");
        }
        
        return props;
    }
    
    /**
     * Configures the connection with platform-specific settings.
     */
    private static void configureConnection(Connection conn, DatabasePlatform platform) throws SQLException {
        // Set auto-commit based on platform requirements
        conn.setAutoCommit(platform.isAutoCommit());
        
        // Apply platform-specific SQL configurations
        if (platform.requiresAnsiMode()) {
            SQLService.simpleUpdate(conn, ANSI_SQL_MODE, new ArrayList<>(), false);
        }
    }
    
    /**
     * Establishes a connection with retry logic for improved reliability.
     *
     * @param platform The database platform
     * @param destType Type of destination (e.g., source, target)
     * @param maxRetries Maximum number of connection attempts
     * @param retryDelayMs Delay between retry attempts in milliseconds
     * @return Connection object to the database, null if all attempts fail
     */
    public static Connection getConnectionWithRetry(String platform, String destType, 
                                                   int maxRetries, long retryDelayMs) {
        Objects.requireNonNull(platform, "Platform cannot be null");
        Objects.requireNonNull(destType, "Destination type cannot be null");
        
        if (maxRetries < 1) {
            throw new IllegalArgumentException("Max retries must be at least 1");
        }
        
        Connection conn = null;
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                conn = getConnection(platform, destType);
                if (conn != null && isConnectionValid(conn)) {
                    Logging.write("info", THREAD_NAME, 
                        String.format("Connection established on attempt %d/%d", attempt, maxRetries));
                    return conn;
                }
            } catch (Exception e) {
                lastException = e;
                Logging.write("warning", THREAD_NAME, 
                    String.format("Connection attempt %d/%d failed: %s", attempt, maxRetries, e.getMessage()));
            }
            
            // Close failed connection
            if (conn != null) {
                closeDatabaseConnection(conn);
                conn = null;
            }
            
            // Wait before retry (except on last attempt)
            if (attempt < maxRetries && retryDelayMs > 0) {
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    Logging.write("warning", THREAD_NAME, "Connection retry interrupted");
                    break;
                }
            }
        }
        
        Logging.write("severe", THREAD_NAME, 
            String.format("Failed to establish connection after %d attempts", maxRetries));
        
        if (lastException != null) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Last connection error: %s", lastException.getMessage()));
        }
        
        return null;
    }
    
    /**
     * Tests a database connection by executing a simple query.
     *
     * @param conn Database connection to test
     * @return true if connection test succeeds, false otherwise
     */
    public static boolean testConnection(Connection conn) {
        if (!isConnectionValid(conn)) {
            return false;
        }
        
        try {
            // Execute a simple query to test the connection
            SQLService.simpleSelect(conn, "SELECT 1", new ArrayList<>());
            return true;
        } catch (Exception e) {
            Logging.write("warning", THREAD_NAME, 
                String.format("Connection test failed: %s", e.getMessage()));
            return false;
        }
    }
    
    /**
     * Gets connection information for debugging purposes.
     *
     * @param conn Database connection
     * @return String containing connection metadata
     */
    public static String getConnectionInfo(Connection conn) {
        if (conn == null) {
            return "Connection is null";
        }
        
        try {
            return String.format("Database: %s, URL: %s, AutoCommit: %s, ReadOnly: %s, Valid: %s",
                conn.getCatalog(),
                conn.getMetaData().getURL(),
                conn.getAutoCommit(),
                conn.isReadOnly(),
                isConnectionValid(conn));
        } catch (SQLException e) {
            return String.format("Error getting connection info: %s", e.getMessage());
        }
    }
}