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

import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableColumn;
import com.crunchydata.models.DCTableColumnMap;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.services.SQLService;
import com.crunchydata.util.Logging;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.util.SQLConstantsRepo.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Controller class that provides a simplified interface for repository operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 * @version 1.0
 */
public class RepoController {

    private static final String THREAD_NAME = "repo-ctrl";


    /**
     * Completes the table history record in the database using the optimized TableManagementService.
     *
     * @param conn         Database connection
     * @param tid          Table ID
     * @param batchNbr     Batch number
     * @param rowCount     Number of rows processed
     * @param actionResult JSON string representing the action result
     */
    public void completeTableHistory(Connection conn, Integer tid, Integer batchNbr, Integer rowCount, String actionResult) {
        try {
            TableManagementService.completeTableHistory(conn, tid, batchNbr, rowCount, actionResult);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error completing table history: %s", e.getMessage()));
            throw new RuntimeException("Failed to complete table history", e);
        }
    }

    /**
     * Creates a staging table for data comparison using the optimized StagingOperationsService.
     *
     * @param conn          Database connection
     * @param location      Location identifier (source or target)
     * @param tid           Table ID
     * @param threadNbr     Thread number
     * @return              The name of the created staging table
     */
    public String createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr) {
        try {
            return StagingOperationsService.createStagingTable(conn, location, tid, threadNbr);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error creating staging table: %s", e.getMessage()));
            throw new RuntimeException("Failed to create staging table", e);
        }
    }

    /**
     * Deletes data comparison results from the specified table.
     *
     * @param conn      Database connection
     * @param tid       Table ID
     * @param batchNbr  Batch number
     */
    public void deleteDataCompare(Connection conn, Integer tid, Integer batchNbr) {
        try {
            TableManagementService.deleteDataCompare(conn, tid, batchNbr);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error deleting data comparison results: %s", e.getMessage()));
            throw new RuntimeException("Failed to delete data comparison results", e);
        }
    }

    /**
     * Drops the specified staging table.
     *
     * @param conn          Database connection
     * @param stagingTable  Staging table name
     */
    public void dropStagingTable(Connection conn, String stagingTable) {
        try {
            StagingOperationsService.dropStagingTable(conn, stagingTable);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error dropping staging table: %s", e.getMessage()));
            throw new RuntimeException("Failed to drop staging table", e);
        }
    }

    /**
     * Gets the project settings from dc_project table.
     *
     * @param conn        Database connection
     * @param pid         Project ID
     * @return String     Contents of project_config field
     */
    public static String getProjectConfig(Connection conn, Integer pid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(pid);
        return SQLService.simpleSelectReturnString(conn, SQL_REPO_DCPROJECT_GETBYPID, binds);
    }


    /**
     * Retrieves table information from the database using the optimized TableManagementService.
     *
     * @param pid       Project ID
     * @param conn      Database connection
     * @param batchNbr  Batch number
     * @param table     Table name filter
     * @param check     Check flag for filtering results
     * @return CachedRowSet containing the result set
     */
    public CachedRowSet getTables(Integer pid, Connection conn, Integer batchNbr, String table, Boolean check) {
        try {
            return TableManagementService.getTables(pid, conn, batchNbr, table, check);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error getting tables: %s", e.getMessage()));
            throw new RuntimeException("Failed to get tables", e);
        }
    }

    /**
     * Loads findings from the staging table into the main table using the optimized StagingOperationsService.
     *
     * @param conn         Database connection
     * @param location     Location identifier (source or target)
     * @param tid          Table ID
     * @param tableAlias   Table alias
     * @param stagingTable Staging table name
     * @param batchNbr     Batch number
     * @param threadNbr    Thread number
     */
    public void loadFindings(Connection conn, String location, Integer tid, String tableAlias, String stagingTable, Integer batchNbr, Integer threadNbr) {
        try {
            StagingOperationsService.loadFindings(conn, location, tid, stagingTable, batchNbr, threadNbr, tableAlias);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error loading findings: %s", e.getMessage()));
            throw new RuntimeException("Failed to load findings", e);
        }
    }

    /**
     * Saves the table information to the database using the optimized TableManagementService.
     *
     * @param conn      Database connection
     * @param dcTable   Table model
     * @return Updated table model with generated ID
     */
    public static DCTable saveTable(Connection conn, DCTable dcTable) {
        try {
            return TableManagementService.saveTable(conn, dcTable);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error saving table: %s", e.getMessage()));
            throw new RuntimeException("Failed to save table", e);
        }
    }

    /**
     * Saves table map information to the database using the optimized TableManagementService.
     *
     * @param conn        Database connection
     * @param dcTableMap  Table Map record
     */
    public static void saveTableMap(Connection conn, DCTableMap dcTableMap) {
        try {
            TableManagementService.saveTableMap(conn, dcTableMap);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error saving table map: %s", e.getMessage()));
            throw new RuntimeException("Failed to save table map", e);
        }
    }

    /**
     * Saves DCTableColumn to dc_table_column using the optimized ColumnManagementService.
     *
     * @param conn      Database connection
     * @param dctc      Table Column record
     * @return Updated table column model with generated ID
     */
    public static DCTableColumn saveTableColumn(Connection conn, DCTableColumn dctc) {
        try {
            return ColumnManagementService.saveTableColumn(conn, dctc);
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, 
                String.format("Error saving table column: %s", e.getMessage()));
            throw new RuntimeException("Failed to save table column", e);
        }
    }

    /**
     * Saves DCTableColumnMap to dc_table_column_map.
     *
     * @param conn               Database connection
     * @param dctcm             Table Column Map record.
     */
    public static void saveTableColumnMap (Connection conn, DCTableColumnMap dctcm) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, dctcm.getTid());
        binds.add(1, dctcm.getColumnID());
        binds.add(2, dctcm.getColumnOrigin());
        binds.add(3,dctcm.getColumnName());
        binds.add(4,dctcm.getDataType());
        binds.add(5,dctcm.getDataClass());
        binds.add(6,dctcm.getDataLength());
        binds.add(7,dctcm.getNumberPrecision());
        binds.add(8,dctcm.getNumberScale());
        binds.add(9,dctcm.getColumnNullable());
        binds.add(10,dctcm.getColumnPrimaryKey());
        binds.add(11,dctcm.getMapExpression());
        binds.add(12,dctcm.getSupported());
        binds.add(13,dctcm.getPreserveCase());

        SQLService.simpleUpdate(conn,SQL_REPO_DCTABLECOLUMNMAP_INSERT,binds,true);
    }


    /**
     * Starts a new table history record.
     *
     * @param conn        Database connection
     * @param tid         Table ID
     * @param batchNbr    Batch number
     */
    public void startTableHistory (Connection conn, Integer tid, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tid);
        binds.add(1,batchNbr);
        SQLService.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_INSERT, binds, true);
    }

    /**
     * Creates a new data comparison result record.
     *
     * @param conn      Database connection
     * @param tableName Table name
     * @param rid       Reconciliation ID
     * @return The comparison ID (cid)
     */
    public Integer dcrCreate (Connection conn, int tid, String tableName, long rid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, tid);
        binds.add(1, tableName);
        binds.add(2, rid);

        CachedRowSet crs = SQLService.simpleUpdateReturning(conn, SQL_REPO_DCRESULT_INSERT, binds);
        int cid = -1;
        try {
            while (crs.next()) {
                cid = crs.getInt(1);
            }

            crs.close();

        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error retrieving cid at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        return cid;
    }

    /**
     * Updates the row count in the data comparison result record.
     *
     * @param conn      Database connection
     * @param targetType Type of target ("source" or "target")
     * @param cid       Comparison ID
     * @param rowCount  Row count to update
     */
    public void dcrUpdateRowCount (Connection conn, String targetType, Integer cid, Integer rowCount) {
        String sql;

        // Dynamic SQL
        if (targetType.equals("source")) {
            sql = "UPDATE dc_result SET source_cnt=source_cnt+? WHERE cid=?";
        } else {
            sql = "UPDATE dc_result SET target_cnt=target_cnt+? WHERE cid=?";
        }

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,rowCount);
        binds.add(1,cid);

        SQLService.simpleUpdate(conn,sql,binds, true);
    }

    /**
     * Vacuum Repo Tables
     *
     * @param conn      Database connection
     */
    public static void vacuumRepo(Connection conn) {

        try {
            boolean autoCommit = conn.getAutoCommit();

            conn.setAutoCommit(true);

            SQLService.simpleExecute(conn, "VACUUM dc_table");
            SQLService.simpleExecute(conn, "VACUUM dc_table_map");
            SQLService.simpleExecute(conn, "VACUUM dc_table_column");
            SQLService.simpleExecute(conn, "VACUUM dc_table_column_map");

            conn.setAutoCommit(autoCommit);

        } catch (Exception e) {
            // do nothing
        }
    }
}
