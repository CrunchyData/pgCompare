package com.crunchydata.controller;

import com.crunchydata.model.DataCompare;
import com.crunchydata.util.Logging;
import com.crunchydata.services.dbPostgres;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import static com.crunchydata.util.Settings.Props;

public class RepoController {

    public static void completeTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr, Integer rowCount, String actionResult) {
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

    public static void deleteDataCompare(Connection conn, String location, String  table, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,table);
        binds.add(1,batchNbr);

        String sql = "DELETE from dc_" + location +" WHERE table_name=? and batch_nbr=?";

        dbPostgres.simpleUpdate(conn, sql, binds, true);

    }

    public static CachedRowSet getTables(Connection conn, Integer batchNbr, String table, Boolean check) {
        ArrayList<Object> binds = new ArrayList<>();
        int bindCount = 1;
        binds.add(0,"ready");

        String sql = """
                     SELECT tid, source_schema, source_table,
                            target_schema, target_table, table_filter,
                            parallel_degree, status, batch_nbr, mod_column
                     FROM dc_table
                     WHERE status=?
                     """;

        if ( batchNbr > 0 ) {
            binds.add(bindCount, batchNbr);
            bindCount++;
            sql += " AND batch_nbr=?";
        }

        if (!table.isEmpty()) {
            binds.add(bindCount,table);
            bindCount++;
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

    public static void loadDataCompare (Connection conn, String location,List<DataCompare> list) {
        try {
                int cnt = 0;
                String sql = "INSERT INTO dc_" + location + " (table_name, pk_hash, column_hash, pk, thread_nbr, batch_nbr) VALUES (?,?,?,(?)::jsonb,?,?)";
                conn.setAutoCommit(false);
                PreparedStatement stmt = conn.prepareStatement(sql);
                for (DataCompare dc: list) {
                   stmt.setString(1, dc.getTableName());
                   stmt.setString(2,dc.getPkHash());
                   stmt.setString(3,dc.getColumnHash());
                   stmt.setString(4,dc.getPk().replace(",}","}"));
                   stmt.setInt(5, dc.getThreadNbr());
                   stmt.setInt(6, dc.getBatchNbr());
                   stmt.addBatch();

                   cnt++;

                   if (cnt % Integer.parseInt(Props.getProperty("batch-commit-size")) == 0 || cnt == list.size()) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                        conn.commit();
                   }
                }

                conn.commit();

                stmt.close();

        } catch (Exception e) {
            Logging.write("severe", "repo-controller", "Error loading compare data into repo " + e.getMessage());
        }
    }

    public static void startTableHistory (Connection conn, Integer tid, String actionType, Integer batchNbr) {
        ArrayList<Object> binds = new ArrayList<>();
        String sql = "INSERT INTO dc_table_history (tid, action_type, start_dt, load_id, batch_nbr, row_count) VALUES (?, ?, current_timestamp, ?, ?, 0)";
        binds.add(0,tid);
        binds.add(1,actionType);
        binds.add(2,"reconcile");
        binds.add(3,batchNbr);
        dbPostgres.simpleUpdate(conn, sql, binds, true);
    }

    public static Integer dcrCreate (Connection conn, String tableName, long rid) {
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
        } catch (Exception e) {
            Logging.write("severe", "repo-controller", "Error retrieving cid");
        }

        return cid;
    }

    public static void dcrUpdateRowCount (Connection conn, String targetType, Integer cid, Integer rowCount) {
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
