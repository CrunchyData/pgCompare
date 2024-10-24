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

import com.crunchydata.model.DCTable;
import com.crunchydata.model.DCTableColumn;
import com.crunchydata.model.DCTableColumnMap;
import com.crunchydata.model.DCTableMap;
import com.crunchydata.services.dbCommon;
import com.crunchydata.util.Logging;

import static com.crunchydata.util.SQLConstantsRepo.*;
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
                    tid     int8 NOT NULL,
                	pk_hash varchar(100) NULL,
                	column_hash varchar(100) NULL,
                	pk jsonb NULL,
                	compare_result bpchar(1) NULL,
                	CONSTRAINT dc_source_pk PRIMARY KEY (tid, pk_hash)
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
     * @param tid       Table ID
     * @param batchNbr  Batch number
     */
    public void deleteDataCompare(Connection conn, Integer tid, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tid);
        binds.add(1,batchNbr);

        try {
            dbCommon.simpleUpdate(conn,SQL_REPO_DCSOURCE_DELETEBYTIDBATCHNBR, binds, true);
            dbCommon.simpleUpdate(conn,SQL_REPO_DCTARGET_DELETEBYTIDBATCHNBR, binds, true);

            boolean currentAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            binds.clear();
            dbCommon.simpleUpdate(conn, "vacuum dc_source", binds, false);
            dbCommon.simpleUpdate(conn, "vacuum dc_target", binds, false);
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
     * Gets the project settings from dc_project table.
     *
     * @param conn        Database connection
     * @param pid         Project ID
     * @return String     Contents of project_config field
     */
    public static String getProjectConfig (Connection conn, Integer pid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, pid);

        return dbCommon.simpleSelectReturnString(conn, "SELECT coalesce(project_config,'{}') project_config FROM dc_project WHERE pid=?", binds);
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
    public CachedRowSet getTables(Integer pid, Connection conn, Integer batchNbr, String table, Boolean check) {

        String sql = SQL_REPO_DCTABLE_SELECTBYPID;

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, pid);

        if ( batchNbr > 0 ) {
            binds.add(binds.size(), batchNbr);
            sql += " AND batch_nbr=?";
        }

        if (!table.isEmpty()) {
            binds.add(binds.size(),table);
            sql += " AND table_alias=?";
        }

        if (check) {
            sql += """ 
                    AND (tid IN (SELECT tid FROM dc_target WHERE compare_result != 'e')
                         OR  tid IN (SELECT tid FROM dc_source WHERE compare_result != 'e'))
                   """;
        }

        sql += " ORDER BY table_alias";
        
        return dbCommon.simpleSelect(conn, sql, binds);

    }

    /**
     * Loads findings from the staging table into the main table.
     *
     * @param conn         Database connection
     * @param location     Location identifier (source or target)
     * @param stagingTable Staging table name
     * @param batchNbr     Batch number
     * @param threadNbr    Thread number
     */
    public void loadFindings (Connection conn, String location, Integer tid, String stagingTable, Integer batchNbr, Integer threadNbr) {

        String sqlFinal = SQL_REPO_DCSOURCE_INSERT.replaceAll("dc_source", String.format("dc_%s",location)).replaceAll("stagingtable", stagingTable);

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, tid);
        binds.add(1, threadNbr);
        binds.add(2, batchNbr);
        dbCommon.simpleUpdate(conn, sqlFinal, binds, true);
    }

    /**
     * Saves the table information to the database.
     *
     * @param conn      Database connection
     * @param dcTable   Table model
     */
    public static DCTable saveTable (Connection conn, DCTable dcTable) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, dcTable.getPid());
        binds.add(1, dcTable.getTableAlias());

        Integer tid = dbCommon.simpleUpdateReturningInteger(conn,SQL_REPO_DCTABLE_INSERT,binds);

        dcTable.setTid(tid);

        return dcTable;
    }

    /**
     * Starts table detail for table map.
     *
     * @param conn        Database connection
     * @param dcTableMap  Table Map record.
     */
    public static void saveTableMap (Connection conn, DCTableMap dcTableMap) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, dcTableMap.getTid());
        binds.add(1, dcTableMap.getDestType());
        binds.add(2,dcTableMap.getSchemaName());
        binds.add(3,dcTableMap.getTableName());
        binds.add(4,dcTableMap.isSchemaPreserveCase());
        binds.add(5,dcTableMap.isTablePreserveCase());

        dbCommon.simpleUpdate(conn,SQL_REPO_DCTABLEMAP_INSERT,binds,true);
    }

    /**
     * Saves DCTableColumn to dc_table_column.
     *
     * @param conn      Database connection
     * @param dctc      Table Column record
     */
    public static DCTableColumn saveTableColumn (Connection conn, DCTableColumn dctc) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, dctc.getTid());
        binds.add(1, dctc.getColumnAlias());

        Integer cid = dbCommon.simpleUpdateReturningInteger(conn,SQL_REPO_DCTABLECOLUMN_INSERT,binds);

        dctc.setColumnID(cid);

        return dctc;
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
        binds.add(7,dctcm.getNumberPrecission());
        binds.add(8,dctcm.getNumberScale());
        binds.add(9,dctcm.getColumnNullable());
        binds.add(10,dctcm.getColumnPrimaryKey());
        binds.add(11,dctcm.getMapExpression());
        binds.add(12,dctcm.getSupported());
        binds.add(13,dctcm.getPreserveCase());

        dbCommon.simpleUpdate(conn,SQL_REPO_DCTABLECOLUMNMAP_INSERT,binds,true);
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
    public Integer dcrCreate (Connection conn, int tid, String tableName, long rid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, tid);
        binds.add(1, tableName);
        binds.add(2, rid);

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
