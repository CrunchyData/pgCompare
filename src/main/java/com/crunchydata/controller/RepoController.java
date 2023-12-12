/*
 * Copyright 2012-2023 the original author or authors.
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

import com.crunchydata.util.Logging;
import com.crunchydata.services.dbPostgres;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.util.ArrayList;

public class RepoController {

    public void completeTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr, Integer rowCount, String actionResult) {
        ArrayList<Object> binds = new ArrayList<>();
        String sql = "UPDATE dc_table_history set end_dt=current_timestamp, row_count=?, action_result=?::jsonb WHERE tid=? AND action_type=? and load_id=? and batch_nbr=?";
        binds.add(0,rowCount);
        binds.add(1,actionResult);
        binds.add(2,tid);
        binds.add(3,actionType);
        binds.add(4,"reconcile");
        binds.add(5,batchNbr);
        dbPostgres.simpleUpdate(conn, sql, binds, true);
    }

    public String createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr) {
        String sql = """
                CREATE UNLOGGED TABLE dc_source (
                	table_name varchar(30) NULL,
                	thread_nbr int4 NULL,
                	batch_nbr int4 NULL,
                	pk_hash varchar(40) NULL,
                	column_hash varchar(40) NULL,
                	pk jsonb NULL,
                	compare_result bpchar(1) NULL
                ) with (autovacuum_enabled=false)
                """;

        String stagingTable = "dc_" + location + "_" + tid + "_" + threadNbr;

        sql = sql.replaceAll("dc_source",stagingTable);

        dropStagingTable(conn, stagingTable);

        dbPostgres.simpleExecute(conn, sql);

        return stagingTable;
    }

    public void deleteDataCompare(Connection conn, String location, String  table, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,table);
        binds.add(1,batchNbr);

        String sql = "DELETE from dc_" + location +" WHERE table_name=? and batch_nbr=?";

        try {
            dbPostgres.simpleUpdate(conn, sql, binds, true);

            boolean currentAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            binds.clear();
            dbPostgres.simpleUpdate(conn, "vacuum dc_" + location, binds, false);
            conn.setAutoCommit(currentAutoCommit);
        } catch (Exception e) {
            System.out.println("Error clearing staging tables");
            e.printStackTrace();
            System.exit(1);
        }

    }

    public void dropStagingTable(Connection conn, String stagingTable) {
        String sql = "DROP TABLE IF EXISTS " + stagingTable;

        dbPostgres.simpleExecute(conn, sql);

    }


    public CachedRowSet getTables(Connection conn, Integer batchNbr, String table, Boolean check) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,"ready");

        String sql = """
                     SELECT tid, source_schema, source_table,
                            target_schema, target_table, table_filter,
                            parallel_degree, status, batch_nbr, mod_column
                     FROM dc_table
                     WHERE status=?
                     """;

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
        
        return dbPostgres.simpleSelect(conn, sql, binds);

    }

    public void loadFindings (Connection conn, String location, String stagingTable) {
        ArrayList<Object> binds = new ArrayList<>();
        String sqlLoadFindings = """
                INSERT INTO dc_source (table_name, thread_nbr, pk_hash, column_hash, pk, compare_result, batch_nbr) (SELECT table_name, thread_nbr, pk_hash, column_hash, pk, compare_result, batch_nbr FROM stagingtable)
                """;

        String sqlFinal = sqlLoadFindings.replaceAll("dc_source", "dc_"+location).replaceAll("stagingtable", stagingTable);
        dbPostgres.simpleUpdate(conn, sqlFinal, binds, true);
    }

    public void startTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        String sql = "INSERT INTO dc_table_history (tid, action_type, start_dt, load_id, batch_nbr, row_count) VALUES (?, ?, current_timestamp, ?, ?, 0)";
        binds.add(0,tid);
        binds.add(1,actionType);
        binds.add(2,"reconcile");
        binds.add(3,batchNbr);
        dbPostgres.simpleUpdate(conn, sql, binds, true);
    }

    public Integer dcrCreate (Connection conn, String tableName, long rid) {
        ArrayList<Object> binds = new ArrayList<>();
        String sql = "INSERT INTO dc_result (compare_dt, table_name, equal_cnt, missing_source_cnt, missing_target_cnt, not_equal_cnt, source_cnt, target_cnt, status, rid) values (current_timestamp, ?, 0, 0, 0, 0, 0, 0, 'running', ?) returning cid";
        binds.add(0,tableName);
        binds.add(1,rid);
        CachedRowSet crs = dbPostgres.simpleUpdateReturning(conn, sql, binds);
        int cid = -1;
        try {
            while (crs.next()) {
                cid = crs.getInt(1);
            }

            crs.close();

        } catch (Exception e) {
            Logging.write("severe", "repo-controller", "Error retrieving cid");
        }

        return cid;
    }

    public void dcrUpdateRowCount (Connection conn, String targetType, Integer cid, Integer rowCount) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,rowCount);
        binds.add(1,cid);
        String sql;
        if (targetType.equals("source")) {
            sql = "UPDATE dc_result SET source_cnt=source_cnt+? WHERE cid=?";
        } else {
            sql = "UPDATE dc_result SET target_cnt=target_cnt+? WHERE cid=?";
        }

        dbPostgres.simpleUpdate(conn,sql,binds, true);
    }


}
