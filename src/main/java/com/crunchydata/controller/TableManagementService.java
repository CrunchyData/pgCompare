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

import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.service.SQLExecutionService;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Service class for managing table-related operations in the repository.
 * This class encapsulates the logic for table creation, retrieval, and management
 * operations, providing a clean interface for table-related database operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class TableManagementService {
    
    private static final String THREAD_NAME = "table-management";
    
    /**
     * Save table information to the database.
     * 
     * @param conn Database connection
     * @param dataComparisonTable Table model to save
     * @return Updated table model with generated ID
     * @throws SQLException if database operations fail
     */
    public static DataComparisonTable saveTable(Connection conn, DataComparisonTable dataComparisonTable) throws SQLException {
        validateTableInputs(conn, dataComparisonTable);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(dataComparisonTable.getPid());
        binds.add(dataComparisonTable.getTableAlias());
        
        Integer tid = SQLExecutionService.simpleUpdateReturningInteger(conn, SQL_REPO_DCTABLE_INSERT, binds);
        dataComparisonTable.setTid(tid);
        
        LoggingUtils.write("info", THREAD_NAME, String.format("Table saved with ID: %d", tid));
        return dataComparisonTable;
    }
    
    /**
     * Save table map information to the database.
     * 
     * @param conn Database connection
     * @param dataComparisonTableMap Table map model to save
     * @throws SQLException if database operations fail
     */
    public static void saveTableMap(Connection conn, DataComparisonTableMap dataComparisonTableMap) throws SQLException {
        validateTableMapInputs(conn, dataComparisonTableMap);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(dataComparisonTableMap.getTid());
        binds.add(dataComparisonTableMap.getDestType());
        binds.add(dataComparisonTableMap.getSchemaName());
        binds.add(dataComparisonTableMap.getTableName());
        binds.add(dataComparisonTableMap.isSchemaPreserveCase());
        binds.add(dataComparisonTableMap.isTablePreserveCase());
        
        SQLExecutionService.simpleUpdate(conn, SQL_REPO_DCTABLEMAP_INSERT, binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Table map saved for table ID: %d, type: %s", 
                dataComparisonTableMap.getTid(), dataComparisonTableMap.getDestType()));
    }
    
    /**
     * Retrieve table information from the database.
     * 
     * @param pid Project ID
     * @param conn Database connection
     * @param batchNbr Batch number filter
     * @param table Table name filter
     * @param check Check flag for filtering results
     * @return CachedRowSet containing table information
     * @throws SQLException if database operations fail
     */
    public static CachedRowSet getTables(Integer pid, Connection conn, Integer batchNbr, String table, Boolean check) 
            throws SQLException {
        validateGetTablesInputs(pid, conn, batchNbr, table, check);
        
        String sql = buildGetTablesSQL(batchNbr, table, check);
        ArrayList<Object> binds = buildGetTablesBinds(pid, batchNbr, table);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Retrieving tables for project %d, batch %d, table filter: %s", 
                pid, batchNbr, table));
        
        return SQLExecutionService.simpleSelect(conn, sql, binds);
    }
    
    /**
     * Complete table history record in the database.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param batchNbr Batch number
     * @param rowCount Number of rows processed
     * @param actionResult JSON string representing the action result
     * @throws SQLException if database operations fail
     */
    public static void completeTableHistory(Connection conn, Integer tid, Integer batchNbr, 
                                          Integer rowCount, String actionResult) throws SQLException {
        validateCompleteTableHistoryInputs(conn, tid, batchNbr, rowCount, actionResult);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(rowCount);
        binds.add(actionResult);
        binds.add(tid);
        binds.add(batchNbr);
        
        SQLExecutionService.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_UPDATE, binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Table history completed for table %d, batch %d, rows: %d", 
                tid, batchNbr, rowCount));
    }
    
    /**
     * Delete data comparison results from the specified table.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param batchNbr Batch number
     * @throws SQLException if database operations fail
     */
    public static void deleteDataCompare(Connection conn, Integer tid, Integer batchNbr) throws SQLException {
        validateDeleteDataCompareInputs(conn, tid, batchNbr);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(batchNbr);
        
        SQLExecutionService.simpleUpdate(conn, "DELETE FROM dc_source WHERE tid=? AND batch_nbr=?", binds, true);
        SQLExecutionService.simpleUpdate(conn, "DELETE FROM dc_target WHERE tid=? AND batch_nbr=?", binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Data comparison results deleted for table %d, batch %d", tid, batchNbr));
    }
    
    /**
     * Build SQL query for retrieving tables.
     * 
     * @param batchNbr Batch number filter
     * @param table Table name filter
     * @param check Check flag for filtering results
     * @return SQL query string
     */
    private static String buildGetTablesSQL(Integer batchNbr, String table, Boolean check) {
        String sql = SQL_REPO_DCTABLE_SELECTBYPID;
        
        if (batchNbr > 0) {
            sql += " AND batch_nbr=?";
        }
        
        if (!table.isEmpty()) {
            sql += " AND table_alias=?";
        }
        
        if (check) {
            sql += """ 
                    AND (tid IN (SELECT tid FROM dc_target WHERE compare_result != 'e')
                         OR  tid IN (SELECT tid FROM dc_source WHERE compare_result != 'e'))
                   """;
        }
        
        sql += " ORDER BY table_alias";
        return sql;
    }
    
    /**
     * Build parameter list for getTables query.
     * 
     * @param pid Project ID
     * @param batchNbr Batch number filter
     * @param table Table name filter
     * @return Parameter list
     */
    private static ArrayList<Object> buildGetTablesBinds(Integer pid, Integer batchNbr, String table) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(pid);
        
        if (batchNbr > 0) {
            binds.add(batchNbr);
        }
        
        if (!table.isEmpty()) {
            binds.add(table);
        }
        
        return binds;
    }
    
    /**
     * Validate inputs for saveTable method.
     * 
     * @param conn Database connection
     * @param dataComparisonTable Table model
     */
    private static void validateTableInputs(Connection conn, DataComparisonTable dataComparisonTable) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (dataComparisonTable == null) {
            throw new IllegalArgumentException("Table model cannot be null");
        }
        if (dataComparisonTable.getPid() == null || dataComparisonTable.getPid() <= 0) {
            throw new IllegalArgumentException("Project ID must be a positive integer");
        }
        if (dataComparisonTable.getTableAlias() == null || dataComparisonTable.getTableAlias().trim().isEmpty()) {
            throw new IllegalArgumentException("Table alias cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for saveTableMap method.
     * 
     * @param conn Database connection
     * @param dataComparisonTableMap Table map model
     */
    private static void validateTableMapInputs(Connection conn, DataComparisonTableMap dataComparisonTableMap) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (dataComparisonTableMap == null) {
            throw new IllegalArgumentException("Table map model cannot be null");
        }
        if (dataComparisonTableMap.getTid() == null || dataComparisonTableMap.getTid() <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (dataComparisonTableMap.getDestType() == null || dataComparisonTableMap.getDestType().trim().isEmpty()) {
            throw new IllegalArgumentException("Destination type cannot be null or empty");
        }
        if (dataComparisonTableMap.getSchemaName() == null || dataComparisonTableMap.getSchemaName().trim().isEmpty()) {
            throw new IllegalArgumentException("Schema name cannot be null or empty");
        }
        if (dataComparisonTableMap.getTableName() == null || dataComparisonTableMap.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for getTables method.
     * 
     * @param pid Project ID
     * @param conn Database connection
     * @param batchNbr Batch number
     * @param table Table name
     * @param check Check flag
     */
    private static void validateGetTablesInputs(Integer pid, Connection conn, Integer batchNbr, 
                                               String table, Boolean check) {
        if (pid == null || pid <= 0) {
            throw new IllegalArgumentException("Project ID must be a positive integer");
        }
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (batchNbr == null || batchNbr < 0) {
            throw new IllegalArgumentException("Batch number must be non-negative");
        }
        if (table == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        if (check == null) {
            throw new IllegalArgumentException("Check flag cannot be null");
        }
    }
    
    /**
     * Validate inputs for completeTableHistory method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param batchNbr Batch number
     * @param rowCount Row count
     * @param actionResult Action result
     */
    private static void validateCompleteTableHistoryInputs(Connection conn, Integer tid, Integer batchNbr, 
                                                          Integer rowCount, String actionResult) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (batchNbr == null || batchNbr < 0) {
            throw new IllegalArgumentException("Batch number must be non-negative");
        }
        if (rowCount == null || rowCount < 0) {
            throw new IllegalArgumentException("Row count must be non-negative");
        }
        if (actionResult == null) {
            throw new IllegalArgumentException("Action result cannot be null");
        }
    }
    
    /**
     * Validate inputs for deleteDataCompare method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param batchNbr Batch number
     */
    private static void validateDeleteDataCompareInputs(Connection conn, Integer tid, Integer batchNbr) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (batchNbr == null || batchNbr < 0) {
            throw new IllegalArgumentException("Batch number must be non-negative");
        }
    }
}
