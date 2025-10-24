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

import com.crunchydata.model.DataComparisonTableColumn;
import com.crunchydata.model.DataComparisonTableColumnMap;
import com.crunchydata.service.SQLExecutionService;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Service class for managing column-related operations in the repository.
 * This class encapsulates the logic for column creation, retrieval, and management
 * operations, providing a clean interface for column-related database operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class ColumnManagementService {
    
    private static final String THREAD_NAME = "column-management";
    
    /**
     * Save table column information to the database.
     * 
     * @param conn Database connection
     * @param dctc Table column model to save
     * @return Updated table column model with generated ID
     * @throws SQLException if database operations fail
     */
    public static DataComparisonTableColumn saveTableColumn(Connection conn, DataComparisonTableColumn dctc) throws SQLException {
        validateTableColumnInputs(conn, dctc);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(dctc.getTid());
        binds.add(dctc.getColumnAlias());
        
        Integer cid = SQLExecutionService.simpleUpdateReturningInteger(conn, SQL_REPO_DCTABLECOLUMN_INSERT, binds);
        dctc.setColumnID(cid);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Table column saved with ID: %d for table: %d", cid, dctc.getTid()));
        return dctc;
    }
    
    /**
     * Save table column map information to the database.
     * 
     * @param conn Database connection
     * @param dctcm Table column map model to save
     * @throws SQLException if database operations fail
     */
    public static void saveTableColumnMap(Connection conn, DataComparisonTableColumnMap dctcm) throws SQLException {
        validateTableColumnMapInputs(conn, dctcm);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(dctcm.getTid());
        binds.add(dctcm.getColumnID());
        binds.add(dctcm.getColumnOrigin());
        binds.add(dctcm.getColumnName());
        binds.add(dctcm.getDataType());
        binds.add(dctcm.getDataClass());
        binds.add(dctcm.getDataLength());
        binds.add(dctcm.getNumberPrecision());
        binds.add(dctcm.getNumberScale());
        binds.add(dctcm.getColumnNullable());
        binds.add(dctcm.getColumnPrimaryKey());
        binds.add(dctcm.getSupported());
        binds.add(dctcm.getPreserveCase());
        
        SQLExecutionService.simpleUpdate(conn, SQL_REPO_DCTABLECOLUMNMAP_INSERT, binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Table column map saved for column ID: %d, origin: %s", 
                dctcm.getColumnID(), dctcm.getColumnOrigin()));
    }
    
    /**
     * Get column information by table ID and column alias.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param columnAlias Column alias
     * @return Column ID if found, null otherwise
     * @throws SQLException if database operations fail
     */
    public static Integer getColumnByAlias(Connection conn, Integer tid, String columnAlias) throws SQLException {
        validateGetColumnInputs(conn, tid, columnAlias);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(columnAlias);
        
        return SQLExecutionService.simpleSelectReturnInteger(conn, SQL_REPO_DCTABLECOLUMN_SELECTBYTIDALIAS, binds);
    }
    
    /**
     * Get full column mapping by table ID.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @return JSON string containing column mapping
     * @throws SQLException if database operations fail
     */
    public static String getColumnMapping(Connection conn, Integer tid) throws SQLException {
        validateGetColumnMappingInputs(conn, tid);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        
        String result = SQLExecutionService.simpleSelectReturnString(conn, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Column mapping retrieved for table ID: %d", tid));
        
        return result;
    }
    
    /**
     * Delete column mappings by table ID.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @throws SQLException if database operations fail
     */
    public static void deleteColumnMappings(Connection conn, Integer tid) throws SQLException {
        validateDeleteColumnMappingsInputs(conn, tid);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        
        SQLExecutionService.simpleUpdate(conn, "DELETE FROM dc_table_column_map WHERE tid=?", binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Column mappings deleted for table ID: %d", tid));
    }
    
    /**
     * Delete column mappings by table ID and column alias.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param columnAlias Column alias
     * @throws SQLException if database operations fail
     */
    public static void deleteColumnMapping(Connection conn, Integer tid, String columnAlias) throws SQLException {
        validateDeleteColumnMappingInputs(conn, tid, columnAlias);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(columnAlias);
        
        SQLExecutionService.simpleUpdate(conn, "DELETE FROM dc_table_column_map WHERE tid=? AND column_id IN (SELECT column_id FROM dc_table_column WHERE tid=? AND column_alias=?)", binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Column mapping deleted for table ID: %d, alias: %s", tid, columnAlias));
    }
    
    /**
     * Get column information by table ID.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @return CachedRowSet containing column information
     * @throws SQLException if database operations fail
     */
    public static CachedRowSet getColumnsByTableId(Connection conn, Integer tid) throws SQLException {
        validateGetColumnsByTableIdInputs(conn, tid);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Retrieving columns for table ID: %d", tid));
        
        return SQLExecutionService.simpleSelect(conn, "SELECT * FROM dc_table_column WHERE tid=?", binds);
    }
    
    /**
     * Validate inputs for saveTableColumn method.
     * 
     * @param conn Database connection
     * @param dctc Table column model
     */
    private static void validateTableColumnInputs(Connection conn, DataComparisonTableColumn dctc) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (dctc == null) {
            throw new IllegalArgumentException("Table column model cannot be null");
        }
        if (dctc.getTid() == null || dctc.getTid() <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (dctc.getColumnAlias() == null || dctc.getColumnAlias().trim().isEmpty()) {
            throw new IllegalArgumentException("Column alias cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for saveTableColumnMap method.
     * 
     * @param conn Database connection
     * @param dctcm Table column map model
     */
    private static void validateTableColumnMapInputs(Connection conn, DataComparisonTableColumnMap dctcm) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (dctcm == null) {
            throw new IllegalArgumentException("Table column map model cannot be null");
        }
        if (dctcm.getTid() == null || dctcm.getTid() <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (dctcm.getColumnID() == null || dctcm.getColumnID() <= 0) {
            throw new IllegalArgumentException("Column ID must be a positive integer");
        }
        if (dctcm.getColumnOrigin() == null || dctcm.getColumnOrigin().trim().isEmpty()) {
            throw new IllegalArgumentException("Column origin cannot be null or empty");
        }
        if (dctcm.getColumnName() == null || dctcm.getColumnName().trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        if (dctcm.getDataType() == null || dctcm.getDataType().trim().isEmpty()) {
            throw new IllegalArgumentException("Data type cannot be null or empty");
        }
        if (dctcm.getDataClass() == null || dctcm.getDataClass().trim().isEmpty()) {
            throw new IllegalArgumentException("Data class cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for getColumnByAlias method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param columnAlias Column alias
     */
    private static void validateGetColumnInputs(Connection conn, Integer tid, String columnAlias) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (columnAlias == null || columnAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("Column alias cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for getColumnMapping method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     */
    private static void validateGetColumnMappingInputs(Connection conn, Integer tid) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
    }
    
    /**
     * Validate inputs for deleteColumnMappings method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     */
    private static void validateDeleteColumnMappingsInputs(Connection conn, Integer tid) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
    }
    
    /**
     * Validate inputs for deleteColumnMapping method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     * @param columnAlias Column alias
     */
    private static void validateDeleteColumnMappingInputs(Connection conn, Integer tid, String columnAlias) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (columnAlias == null || columnAlias.trim().isEmpty()) {
            throw new IllegalArgumentException("Column alias cannot be null or empty");
        }
    }
    
    /**
     * Validate inputs for getColumnsByTableId method.
     * 
     * @param conn Database connection
     * @param tid Table ID
     */
    private static void validateGetColumnsByTableIdInputs(Connection conn, Integer tid) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (tid == null || tid <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
    }
}
