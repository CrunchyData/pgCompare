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

package com.crunchydata.controller;

import com.crunchydata.services.SQLService;
import com.crunchydata.util.Logging;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.util.SQLConstantsRepo.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Service class for managing staging table operations in the repository.
 * This class encapsulates the logic for staging table creation, management,
 * and data loading operations, providing a clean interface for staging-related operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class StagingOperationsService {
    
    private static final String THREAD_NAME = "staging-operations";
    
    /**
     * Create a staging table for data comparison.
     * 
     * @param conn Database connection
     * @param location Location identifier (source or target)
     * @param tid Table ID
     * @param threadNbr Thread number
     * @return The name of the created staging table
     * @throws SQLException if database operations fail
     */
    public static String createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr) 
            throws SQLException {
        validateCreateStagingTableInputs(conn, location, tid, threadNbr);
        
        String sql = String.format(REPO_DDL_STAGE_TABLE, Props.getProperty("stage-table-parallel"));
        String stagingTable = String.format("dc_%s_%s_%s", location, tid, threadNbr);
        
        sql = sql.replaceAll("dc_source", stagingTable);
        
        // Drop existing staging table if it exists
        dropStagingTable(conn, stagingTable);
        
        // Create new staging table
        SQLService.simpleExecute(conn, sql);
        
        Logging.write("info", THREAD_NAME, 
            String.format("Staging table created: %s for location: %s, table: %d, thread: %d", 
                stagingTable, location, tid, threadNbr));
        
        return stagingTable;
    }
    
    /**
     * Drop a staging table if it exists.
     * 
     * @param conn Database connection
     * @param stagingTable Staging table name
     * @throws SQLException if database operations fail
     */
    public static void dropStagingTable(Connection conn, String stagingTable) throws SQLException {
        validateDropStagingTableInputs(conn, stagingTable);
        
        String sql = String.format("DROP TABLE IF EXISTS %s", stagingTable);
        SQLService.simpleExecute(conn, sql);
        
        Logging.write("debug", THREAD_NAME, 
            String.format("Staging table dropped: %s", stagingTable));
    }
    
    /**
     * Load findings from the staging table into the main table.
     * 
     * @param conn Database connection
     * @param location Location identifier (source or target)
     * @param tid Table ID
     * @param stagingTable Staging table name
     * @param batchNbr Batch number
     * @param threadNbr Thread number
     * @param tableAlias Table alias
     * @throws SQLException if database operations fail
     */
    public static void loadFindings(Connection conn, String location, Integer tid, String stagingTable, 
                                  Integer batchNbr, Integer threadNbr, String tableAlias) throws SQLException {
        validateLoadFindingsInputs(conn, location, tid, stagingTable, batchNbr, threadNbr, tableAlias);
        
        String sqlFinal = SQL_REPO_DCSOURCE_INSERT
            .replaceAll("dc_source", String.format("dc_%s", location))
            .replaceAll("stagingtable", stagingTable);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(threadNbr);
        binds.add(batchNbr);
        binds.add(tableAlias);
        
        SQLService.simpleUpdate(conn, sqlFinal, binds, true);
        
        Logging.write("info", THREAD_NAME, 
            String.format("Findings loaded from staging table %s to main table for location: %s", 
                stagingTable, location));
    }
    
    /**
     * Create a new data comparison result record.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param tableName Table name
     * @param rid Reconciliation ID
     * @return The comparison ID (cid)
     * @throws SQLException if database operations fail
     */
    public static Integer createDataCompareResult(Connection conn, Integer tid, String tableName, Long rid) 
            throws SQLException {
        validateCreateDataCompareResultInputs(conn, tid, tableName, rid);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(tableName);
        binds.add(rid);
        
        Integer cid = SQLService.simpleUpdateReturningInteger(conn, SQL_REPO_DCRESULT_INSERT, binds);
        
        Logging.write("info", THREAD_NAME, 
            String.format("Data comparison result created with CID: %d for table: %s", cid, tableName));
        
        return cid;
    }
    
    /**
     * Update row count in the data comparison result record.
     * 
     * @param conn Database connection
     * @param targetType Type of target ("source" or "target")
     * @param cid Comparison ID
     * @param rowCount Row count to update
     * @throws SQLException if database operations fail
     */
    public static void updateRowCount(Connection conn, String targetType, Integer cid, Integer rowCount) 
            throws SQLException {
        validateUpdateRowCountInputs(conn, targetType, cid, rowCount);
        
        String sql = buildUpdateRowCountSQL(targetType);
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(rowCount);
        binds.add(cid);
        
        SQLService.simpleUpdate(conn, sql, binds, true);
        
        Logging.write("info", THREAD_NAME, 
            String.format("Row count updated for %s: %d rows, CID: %d", targetType, rowCount, cid));
    }
    
    /**
     * Vacuum repository tables for maintenance.
     * 
     * @param conn Database connection
     * @throws SQLException if database operations fail
     */
    public static void vacuumRepository(Connection conn) throws SQLException {
        validateVacuumRepositoryInputs(conn);
        
        try {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            
            SQLService.simpleExecute(conn, "VACUUM dc_table");
            SQLService.simpleExecute(conn, "VACUUM dc_table_map");
            SQLService.simpleExecute(conn, "VACUUM dc_table_column");
            SQLService.simpleExecute(conn, "VACUUM dc_table_column_map");
            
            conn.setAutoCommit(autoCommit);
            
            Logging.write("info", THREAD_NAME, "Repository tables vacuumed successfully");
            
        } catch (Exception e) {
            Logging.write("warning", THREAD_NAME, 
                String.format("Repository vacuum completed with warnings: %s", e.getMessage()));
        }
    }
    
    /**
     * Build SQL query for updating row count.
     * 
     * @param targetType Type of target ("source" or "target")
     * @return SQL query string
     */
    private static String buildUpdateRowCountSQL(String targetType) {
        if ("source".equals(targetType)) {
            return "UPDATE dc_result SET source_cnt=source_cnt+? WHERE cid=?";
        } else {
            return "UPDATE dc_result SET target_cnt=target_cnt+? WHERE cid=?";
        }
    }
    
    /**
     * Validate inputs for createStagingTable method.
     * 
     * @param conn Database connection
     * @param location Location identifier
     * @param tid Table ID
     * @param threadNbr Thread number
     */
    private static void validateCreateStagingTableInputs(Connection conn, String location, Integer tid, Integer threadNbr) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (location == null || location.trim().isEmpty()) {
            throw new IllegalArgumentException("Location cannot be null or empty");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (threadNbr == null || threadNbr < 0) {
            throw new IllegalArgumentException("Thread number must be non-negative");
        }
    }
    
    /**
     * Validate inputs for dropStagingTable method.
     * 
     * @param conn Database connection
     * @param stagingTable Staging table name
     */
    private static void validateDropStagingTableInputs(Connection conn, String stagingTable) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (stagingTable == null || stagingTable.trim().isEmpty()) {
            throw new IllegalArgumentException("Staging table name cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for loadFindings method.
     * 
     * @param conn Database connection
     * @param location Location identifier
     * @param tid Table ID
     * @param stagingTable Staging table name
     * @param batchNbr Batch number
     * @param threadNbr Thread number
     * @param tableAlias Table alias
     */
    private static void validateLoadFindingsInputs(Connection conn, String location, Integer tid, String stagingTable, 
                                                  Integer batchNbr, Integer threadNbr, String tableAlias) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (location == null || location.trim().isEmpty()) {
            throw new IllegalArgumentException("Location cannot be null or empty");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (stagingTable == null || stagingTable.trim().isEmpty()) {
            throw new IllegalArgumentException("Staging table name cannot be null or empty");
        }
        if (batchNbr == null || batchNbr < 0) {
            throw new IllegalArgumentException("Batch number must be non-negative");
        }
        if (threadNbr == null || threadNbr < 0) {
            throw new IllegalArgumentException("Thread number must be non-negative");
        }
        if (tableAlias == null || tableAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("Table alias cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for createDataCompareResult method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param tableName Table name
     * @param rid Reconciliation ID
     */
    private static void validateCreateDataCompareResultInputs(Connection conn, Integer tid, String tableName, Long rid) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (rid == null || rid < 0) {
            throw new IllegalArgumentException("Reconciliation ID must be non-negative");
        }
    }
    
    /**
     * Validate inputs for updateRowCount method.
     * 
     * @param conn Database connection
     * @param targetType Target type
     * @param cid Comparison ID
     * @param rowCount Row count
     */
    private static void validateUpdateRowCountInputs(Connection conn, String targetType, Integer cid, Integer rowCount) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (targetType == null || targetType.trim().isEmpty()) {
            throw new IllegalArgumentException("Target type cannot be null or empty");
        }
        if (cid == null || cid <= 0) {
            throw new IllegalArgumentException("Comparison ID must be a positive integer");
        }
        if (rowCount == null || rowCount < 0) {
            throw new IllegalArgumentException("Row count must be non-negative");
        }
    }
    
    /**
     * Validate inputs for vacuumRepository method.
     * 
     * @param conn Database connection
     */
    private static void validateVacuumRepositoryInputs(Connection conn) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
    }
}
