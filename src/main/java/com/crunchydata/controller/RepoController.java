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
import com.crunchydata.model.DataComparisonTableColumn;
import com.crunchydata.model.DataComparisonTableColumnMap;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.service.StagingTableService;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Controller class that provides a simplified interface for repository operations.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 */
public class RepoController {

    private static final String THREAD_NAME = "repo-ctrl";

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

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(rowCount);
        binds.add(actionResult);
        binds.add(tid);
        binds.add(batchNbr);

        SQLExecutionHelper.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_UPDATE, binds, true);

        LoggingUtils.write("info", THREAD_NAME,
                String.format("Table history completed for table %d, batch %d",
                        tid, batchNbr));
    }

    /**
     * Create compare ID for this reconciliation run.
     *
     * @param connRepo Repository connection
     * @param dctmTarget Target table map
     * @param rid Reconciliation ID
     * @return Compare ID
     * @throws SQLException if database operations fail
     */
    public static Integer createCompareId(Connection connRepo, DataComparisonTableMap dctmTarget, long rid) throws SQLException {
        RepoController rpc = new RepoController();
        return rpc.dcrCreate(connRepo, dctmTarget.getTid(), dctmTarget.getTableAlias(), rid);
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
            ArrayList<Object> binds = new ArrayList<>();
            binds.add(tid);
            binds.add(batchNbr);

            SQLExecutionHelper.simpleUpdate(conn, "DELETE FROM dc_source WHERE tid=? AND batch_nbr=?", binds, true);
            SQLExecutionHelper.simpleUpdate(conn, "DELETE FROM dc_target WHERE tid=? AND batch_nbr=?", binds, true);

            LoggingUtils.write("info", THREAD_NAME,
                    String.format("Data comparison results deleted for table %d, batch %d", tid, batchNbr));

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error deleting data comparison results: %s", e.getMessage()));
            throw new RuntimeException("Failed to delete data comparison results", e);
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
        return SQLExecutionHelper.simpleSelectReturnString(conn, SQL_REPO_DCPROJECT_GETBYPID, binds);
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
            StagingTableService.loadFindings(conn, location, tid, stagingTable, batchNbr, threadNbr, tableAlias);
        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error loading findings: %s", e.getMessage()));
            throw new RuntimeException("Failed to load findings", e);
        }
    }

    /**
     * Saves the table information to the database using the optimized TableManagementService.
     *
     * @param conn      Database connection
     * @param dataComparisonTable   Table model
     * @return Updated table model with generated ID
     */
    public static DataComparisonTable saveTable(Connection conn, DataComparisonTable dataComparisonTable) {
        try {
            ArrayList<Object> binds = new ArrayList<>();
            binds.add(dataComparisonTable.getPid());
            binds.add(dataComparisonTable.getTableAlias());

            Integer tid = SQLExecutionHelper.simpleUpdateReturningInteger(conn, SQL_REPO_DCTABLE_INSERT, binds);
            dataComparisonTable.setTid(tid);

            return dataComparisonTable;

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Error saving table: %s", e.getMessage()));
            throw new RuntimeException("Failed to save table", e);
        }
    }


    /**
     * Saves table map information to the database using the optimized TableManagementService.
     *
     * @param conn        Database connection
     * @param dataComparisonTableMap  Table Map record
     */
    public static void saveTableMap(Connection conn, DataComparisonTableMap dataComparisonTableMap) {
        try {
            ArrayList<Object> binds = new ArrayList<>();
            binds.add(dataComparisonTableMap.getTid());
            binds.add(dataComparisonTableMap.getDestType());
            binds.add(dataComparisonTableMap.getSchemaName());
            binds.add(dataComparisonTableMap.getTableName());
            binds.add(dataComparisonTableMap.isSchemaPreserveCase());
            binds.add(dataComparisonTableMap.isTablePreserveCase());

            SQLExecutionHelper.simpleUpdate(conn, SQL_REPO_DCTABLEMAP_INSERT, binds, true);

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
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
    public static DataComparisonTableColumn saveTableColumn(Connection conn, DataComparisonTableColumn dctc) {
            ArrayList<Object> binds = new ArrayList<>();
            binds.add(dctc.getTid());
            binds.add(dctc.getColumnAlias());

            Integer cid = SQLExecutionHelper.simpleUpdateReturningInteger(conn, SQL_REPO_DCTABLECOLUMN_INSERT, binds);
            dctc.setColumnID(cid);

            return dctc;
    }

    /**
     * Saves DCTableColumnMap to dc_table_column_map.
     *
     * @param conn               Database connection
     * @param dctcm             Table Column Map record.
     */
    public static void saveTableColumnMap (Connection conn, DataComparisonTableColumnMap dctcm) {
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

        SQLExecutionHelper.simpleUpdate(conn,SQL_REPO_DCTABLECOLUMNMAP_INSERT,binds,true);
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
        SQLExecutionHelper.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_INSERT, binds, true);
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

        CachedRowSet crs = SQLExecutionHelper.simpleUpdateReturning(conn, SQL_REPO_DCRESULT_INSERT, binds);
        int cid = -1;
        try {
            while (crs.next()) {
                cid = crs.getInt(1);
            }

            crs.close();

        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error retrieving cid at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
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

        SQLExecutionHelper.simpleUpdate(conn,sql,binds, true);
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

            SQLExecutionHelper.simpleExecute(conn, "VACUUM dc_table");
            SQLExecutionHelper.simpleExecute(conn, "VACUUM dc_table_map");
            SQLExecutionHelper.simpleExecute(conn, "VACUUM dc_table_column");
            SQLExecutionHelper.simpleExecute(conn, "VACUUM dc_table_column_map");

            conn.setAutoCommit(autoCommit);

        } catch (Exception e) {
            // do nothing
        }
    }
}
