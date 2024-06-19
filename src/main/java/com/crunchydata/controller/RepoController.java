/*
 * Copyright 2012-2024 the original author or authors.
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

import java.sql.Connection;
import java.util.ArrayList;
import javax.sql.rowset.CachedRowSet;

import com.crunchydata.services.dbCommon;
import com.crunchydata.util.Logging;

import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Controller class for managing repository operations.
 *
 * @author Brian Pace
 */
public class RepoController {

    private static final String THREAD_NAME = "RepoController";


    /**
     * Completes the table history record in the database.
     *
     * @param conn         Database connection
     * @param tid          Table ID
     * @param actionType   Type of action
     * @param batchNbr     Batch number
     * @param rowCount     Number of rows processed
     * @param actionResult JSON string representing the action result
     */
    public void completeTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr, Integer rowCount, String actionResult) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,rowCount);
        binds.add(1,actionResult);
        binds.add(2,tid);
        binds.add(3,actionType);
        binds.add(4,"reconcile");
        binds.add(5,batchNbr);

        dbCommon.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_UPDATE, binds, true);
    }

    /**
     * Creates a staging table for data comparison.
     *
     * @param conn       Database connection
     * @param location   Location identifier (source or target)
     * @param tid        Table ID
     * @param threadNbr  Thread number
     * @return The name of the created staging table
     */
    public String createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr) {
        // Dynamic SQL
        String sql = """
                CREATE UNLOGGED TABLE dc_source (
                	pk_hash varchar(100) NULL,
                	column_hash varchar(100) NULL,
                	pk jsonb NULL,
                	compare_result bpchar(1) NULL
                ) with (autovacuum_enabled=false, parallel_workers=
                """ + Props.getProperty("stage-table-parallel") + ")";

        String stagingTable = String.format("dc_%s_%s_%s",location,tid,threadNbr);

        sql = sql.replaceAll("dc_source",stagingTable);
        dropStagingTable(conn, stagingTable);
        dbCommon.simpleExecute(conn, sql);

        return stagingTable;
    }

    /**
     * Deletes data comparison results from the specified table.
     *
     * @param conn      Database connection
     * @param location  Location identifier (source or target)
     * @param table     Table name
     * @param batchNbr  Batch number
     */
    public void deleteDataCompare(Connection conn, String location, String  table, Integer batchNbr) {
        // Dynamic SQL
        String sql = String.format("DELETE from dc_%s WHERE table_name=? and batch_nbr=?",location);

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,table);
        binds.add(1,batchNbr);

        try {
            dbCommon.simpleUpdate(conn, sql, binds, true);

            boolean currentAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            binds.clear();
            dbCommon.simpleUpdate(conn, String.format("vacuum dc_%s",location), binds, false);
            conn.setAutoCommit(currentAutoCommit);
        } catch (Exception e) {
            System.out.printf("Error clearing staging tables: %s%n",e.getMessage());
            System.exit(1);
        }

    }

    /**
     * Drops the specified staging table.
     *
     * @param conn          Database connection
     * @param stagingTable  Staging table name
     */
    public void dropStagingTable(Connection conn, String stagingTable) {
        // Dynamic SQL
        String sql = String.format("DROP TABLE IF EXISTS %s",stagingTable);

        dbCommon.simpleExecute(conn, sql);

    }


    /**
     * Retrieves table information from the database.
     *
     * @param conn      Database connection
     * @param batchNbr  Batch number
     * @param table     Table name filter
     * @param check     Check flag for filtering results
     * @return CachedRowSet containing the result set
     */
    public CachedRowSet getTables(Connection conn, Integer batchNbr, String table, Boolean check) {

        String sql = SQL_REPO_DCTABLE_SELECT;

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,"ready");

        if ( batchNbr > 0 ) {
            binds.add(binds.size(), batchNbr);
            sql += " AND batch_nbr=?";
        }

        if (!table.isEmpty()) {
            binds.add(binds.size(),table);
            sql += " AND target_table=?";
        }

        if (check) {
            sql += """ 
                    AND (target_table IN (SELECT table_name FROM dc_target WHERE compare_result != 'e')
                         OR  source_table IN (SELECT table_name FROM dc_source WHERE compare_result != 'e'))
                   """;
        }

        sql += " ORDER BY target_table";
        
        return dbCommon.simpleSelect(conn, sql, binds);

    }

    /**
     * Loads findings from the staging table into the main table.
     *
     * @param conn         Database connection
     * @param location     Location identifier (source or target)
     * @param stagingTable Staging table name
     * @param tableName    Main table name
     * @param batchNbr     Batch number
     * @param threadNbr    Thread number
     */
    public void loadFindings (Connection conn, String location, String stagingTable, String tableName, Integer batchNbr, Integer threadNbr) {

        String sqlFinal = SQL_REPO_DCSOURCE_INSERT.replaceAll("dc_source", String.format("dc_%s",location)).replaceAll("stagingtable", stagingTable);

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tableName);
        binds.add(1,threadNbr);
        binds.add(2,batchNbr);
        dbCommon.simpleUpdate(conn, sqlFinal, binds, true);
    }

    /**
     * Saves the column map to the database.
     *
     * @param conn       Database connection
     * @param tid        Table ID
     * @param columnMap  JSON string representing the column map
     */
    public void saveColumnMap (Connection conn, Integer tid, String columnMap) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,columnMap);
        binds.add(1,tid);

        dbCommon.simpleUpdate(conn,SQL_REPO_DCTABLE_UPDATE_COLUMNMAP,binds, true);
    }

    /**
     * Saves the table information to the database.
     *
     * @param conn      Database connection
     * @param schema    Schema name
     * @param tableName Table name
     */
    public static void saveTable (Connection conn, String schema, String tableName) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,schema);
        binds.add(1,tableName);
        binds.add(2,schema);
        binds.add(3,tableName);

        dbCommon.simpleUpdate(conn,SQL_REPO_DCTABLE_INSERT,binds, true);
    }

    /**
     * Starts a new table history record.
     *
     * @param conn        Database connection
     * @param tid         Table ID
     * @param actionType  Type of action
     * @param batchNbr    Batch number
     */
    public void startTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tid);
        binds.add(1,actionType);
        binds.add(2,"reconcile");
        binds.add(3,batchNbr);
        dbCommon.simpleUpdate(conn, SQL_REPO_DCTABLEHISTORY_INSERT, binds, true);
    }

    /**
     * Creates a new data comparison result record.
     *
     * @param conn      Database connection
     * @param tableName Table name
     * @param rid       Reconciliation ID
     * @return The comparison ID (cid)
     */
    public Integer dcrCreate (Connection conn, String tableName, long rid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tableName);
        binds.add(1,rid);

        CachedRowSet crs = dbCommon.simpleUpdateReturning(conn, SQL_REPO_DCRESULT_INSERT, binds);
        int cid = -1;
        try {
            while (crs.next()) {
                cid = crs.getInt(1);
            }

            crs.close();

        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, "Error retrieving cid");
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

        dbCommon.simpleUpdate(conn,sql,binds, true);
    }
}
