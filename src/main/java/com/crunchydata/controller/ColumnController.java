package com.crunchydata.controller;

import com.crunchydata.core.database.ColumnMetadataBuilder;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.service.ColumnDiscoveryService;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.crunchydata.service.DatabaseMetadataService.getConcatOperator;
import static com.crunchydata.service.DatabaseMetadataService.getQuoteChar;

/**
 * ColumnController class that provides a simplified interface for column operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 * @version 1.0
 */
public class ColumnController {
    private static final String THREAD_NAME = "column-ctrl";

    /**
     * Retrieves column metadata for a given table using the optimized ColumnMetadataBuilder.
     *
     * @param columnMap        JSON object containing column mapping information
     * @param targetType       The target type of columns
     * @param platform         The database platform (e.g., "mssql", "mysql")
     * @param schema           The schema of the table
     * @param table            The name of the table
     * @param useDatabaseHash  Flag to determine whether to use database hash
     * @return                 A ColumnMetadata object containing column information
     */
    public static ColumnMetadata getColumnInfo(JSONObject columnMap, String targetType, String platform, 
                                             String schema, String table, Boolean useDatabaseHash) {
        try {
            // Validate inputs
            validateInputs(columnMap, targetType, platform, schema, table);
            
            // Create builder with platform-specific settings
            String concatOperator = getConcatOperator(platform);
            String quoteChar = getQuoteChar(platform);
            
            ColumnMetadataBuilder builder = new ColumnMetadataBuilder(
                targetType, platform, schema, table, useDatabaseHash, concatOperator, quoteChar);
            
            // Build and return metadata
            return builder.build(columnMap);
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error building column metadata for %s.%s: %s", schema, table, e.getMessage()));
            throw new RuntimeException("Failed to build column metadata", e);
        }
    }
    
    /**
     * Validate input parameters for column metadata building.
     * 
     * @param columnMap Column mapping JSON
     * @param targetType Target type
     * @param platform Database platform
     * @param schema Schema name
     * @param table Table name
     */
    private static void validateInputs(JSONObject columnMap, String targetType, String platform, 
                                     String schema, String table) {
        if (columnMap == null) {
            throw new IllegalArgumentException("Column map cannot be null");
        }
        if (targetType == null || targetType.trim().isEmpty()) {
            throw new IllegalArgumentException("Target type cannot be null or empty");
        }
        if (platform == null || platform.trim().isEmpty()) {
            throw new IllegalArgumentException("Platform cannot be null or empty");
        }
        if (schema == null || schema.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema cannot be null or empty");
        }
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }



    /**
     * Discover columns from source and target databases using the optimized ColumnDiscoveryService.
     *
     * @param props            Properties for application settings
     * @param pid              Project id
     * @param table            Table to discover columns for
     * @param connRepo         Connection to repository
     * @param connSource       Connection to source database
     * @param connTarget       Connection to target database
     */
    public static void discoverColumns(Properties props, Integer pid, String table, 
                                     Connection connRepo, Connection connSource, Connection connTarget) {
        try {
            // Validate inputs
            validateDiscoveryInputs(props, pid, connRepo, connSource, connTarget);
            
            // Use the optimized discovery service
            ColumnDiscoveryService.discoverColumns(props, pid, table, connRepo, connSource, connTarget);
            
            LoggingUtils.write("info", THREAD_NAME,
                String.format("Successfully completed column discovery for project %d", pid));
                
        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Database error during column discovery for project %d: %s", pid, e.getMessage()));
            throw new RuntimeException("Column discovery failed", e);
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Unexpected error during column discovery for project %d: %s", pid, e.getMessage()));
            throw new RuntimeException("Column discovery failed", e);
        }
    }
    
    /**
     * Validate inputs for column discovery.
     * 
     * @param props Application properties
     * @param pid Project ID
     * @param connRepo Repository connection
     * @param connSource Source connection
     * @param connTarget Target connection
     */
    private static void validateDiscoveryInputs(Properties props, Integer pid, 
                                              Connection connRepo, Connection connSource, Connection connTarget) {
        if (props == null) {
            throw new IllegalArgumentException("Properties cannot be null");
        }
        if (pid == null || pid <= 0) {
            throw new IllegalArgumentException("Project ID must be a positive integer");
        }
        if (connRepo == null) {
            throw new IllegalArgumentException("Repository connection cannot be null");
        }
        if (connSource == null) {
            throw new IllegalArgumentException("Source connection cannot be null");
        }
        if (connTarget == null) {
            throw new IllegalArgumentException("Target connection cannot be null");
        }
    }


}
