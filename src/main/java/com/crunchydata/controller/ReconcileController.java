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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.sql.rowset.CachedRowSet;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;
import com.crunchydata.services.*;

import static com.crunchydata.util.DatabaseUtility.*;
import static com.crunchydata.util.Settings.Props;

import org.json.JSONObject;

public class ReconcileController {

    public static JSONObject reconcileData(Connection repoConn, Connection sourceConn, Connection targetConn, String sourceSchema, String sourceTable, String targetSchema, String targetTable, String tableFilter, String modColumn, Integer parallelDegree, long rid, Boolean check, Integer batchNbr, Integer tid, String columnMapping, Boolean mapOnly) {

        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject columnMap = new JSONObject();
        List<dbReconcile> compareList = new ArrayList<>();
        dbReconcile cst;
        dbReconcile ctt;
        List<dbLoader> loaderList = new ArrayList<>();
        List<dbReconcileObserver> observerList = new ArrayList<>();
        BlockingQueue<DataCompare[]> qs = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
        BlockingQueue<DataCompare[]> qt = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
        JSONObject result = new JSONObject();
        dbReconcileObserver rot;
        RepoController rpc = new RepoController();
        ThreadSync ts;

        /////////////////////////////////////////////////
        // SQL
        /////////////////////////////////////////////////
        String sqlUpdateStatus = """
                                 UPDATE dc_result SET missing_source_cnt=?, missing_target_cnt=?, not_equal_cnt=?, status=?
                                 WHERE cid=?
                                 RETURNING equal_cnt, missing_source_cnt, missing_target_cnt, not_equal_cnt, status
                                 """;

        String sqlMarkNESource = """
                                 UPDATE dc_source s SET compare_result = 'n'
                                 WHERE s.table_name=?
                                       AND EXISTS (SELECT 1 FROM dc_target t WHERE t.table_name=? AND s.pk_hash=t.pk_hash AND s.column_hash != t.column_hash)
                                 """;

        String sqlMarkNETarget ="""
                                UPDATE dc_target t SET compare_result = 'n'
                                WHERE t.table_name=?
                                      AND EXISTS (SELECT 1 FROM dc_source s WHERE s.table_name=? AND t.pk_hash=s.pk_hash AND t.column_hash != s.column_hash)
                                """;

        String sqlMarkMissingSource = """
                                      UPDATE dc_target t SET compare_result = 'm'
                                      WHERE t.table_name=?
                                            AND NOT EXISTS (SELECT 1 FROM dc_source s WHERE s.table_name=? AND t.pk_hash=s.pk_hash)
                                      """;

        String sqlMarkMissingTarget = """
                                      UPDATE dc_source s SET compare_result = 'm'
                                      WHERE s.table_name=?
                                            AND NOT EXISTS (SELECT 1 FROM dc_target t WHERE t.table_name=? AND s.pk_hash=t.pk_hash)
                                      """;

        try {
            /////////////////////////////////////////////////
            // Get Column Info and Mapping
            /////////////////////////////////////////////////
            result.put("tableName", targetTable);
            result.put("status", "processing");
            result.put("compareStatus", "processing");

            if (columnMapping.equals("{}") || mapOnly) {
                columnMap = getColumnMap("source", Props.getProperty("source-type"), sourceConn, sourceSchema, sourceTable, columnMap);
                columnMap = getColumnMap("target", Props.getProperty("target-type"), targetConn, targetSchema, targetTable, columnMap);
                rpc.saveColumnMap(repoConn, tid, columnMap.toString());

                if (mapOnly) {
                    Logging.write("info", "reconcile-controller", "Column mapping complete.");
                    System.exit(0);
                }

            } else {
                columnMap = new JSONObject(columnMapping);
            }

            ColumnMetadata ciSource = getColumnInfo(columnMap, "source", Props.getProperty("source-type"), sourceSchema, sourceTable, !check && Boolean.parseBoolean(Props.getProperty("source-database-hash")));
            ColumnMetadata ciTarget = getColumnInfo(columnMap, "target", Props.getProperty("target-type"), targetSchema, targetTable, !check && Boolean.parseBoolean(Props.getProperty("target-database-hash")));

            Logging.write("info", "reconcile-controller", "Source Columns: " + ciSource.columnList);
            Logging.write("info", "reconcile-controller", "Target Columns: " + ciTarget.columnList);

            Integer cid = rpc.dcrCreate(repoConn, targetTable, rid);

            ////////////////////////////////////////
            // Set Source & Target Variables
            ////////////////////////////////////////
            String sqlSource = switch (Props.getProperty("source-type")) {
                case "postgres" ->
                        dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "oracle" ->
                        dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "mysql" ->
                        dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "mssql" ->
                        dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                default -> "";
            };

            String sqlTarget = switch (Props.getProperty("target-type")) {
                case "postgres" ->
                        dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "oracle" ->
                        dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "mysql" ->
                        dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "mssql" ->
                        dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                default -> "";
            };

            Logging.write("info", "reconcile-controller", "Source Compare Hash SQL: " + sqlSource);
            Logging.write("info", "reconcile-controller", "Target Compare Hash SQL: " + sqlTarget);

            if (check) {
                dbReconcileCheck.checkRows(repoConn, sqlSource, sqlTarget, sourceConn, targetConn, sourceTable, targetTable, ciSource, ciTarget, batchNbr, cid);
            } else {
                ////////////////////////////////////////
                // Execute Compare SQL
                ////////////////////////////////////////
                if (ciTarget.pkList.isBlank() || ciTarget.pkList.isEmpty()) {
                    Logging.write("warning", "reconcile-controller", "Table " + targetTable + " has no Primary Key, skipping reconciliation");
                    result.put("status", "skipped");
                    result.put("compareStatus", "skipped");
                    binds.add(0, cid);
                    dbPostgres.simpleUpdate(repoConn, "UPDATE dc_result SET equal_cnt=0,missing_source_cnt=0,missing_target_cnt=0,not_equal_cnt=0,source_cnt=0,target_cnt=0,status='skipped' WHERE cid=?", binds, true);
                } else {
                    Logging.write("info", "reconcile-controller", "Starting compare hash threads");

                    String stagingTableSource;
                    String stagingTableTarget;

                    for (Integer i = 0; i < parallelDegree; i++) {
                        Logging.write("info", "reconcile-controller", "Creating data compare staging tables");
                        stagingTableSource = rpc.createStagingTable(repoConn, "source", tid, i);
                        stagingTableTarget = rpc.createStagingTable(repoConn, "target", tid, i);

                        Logging.write("info", "reconcile-controller", "Starting compare thread " + i);
                        ts = new ThreadSync();
                        rot = new dbReconcileObserver(targetSchema, targetTable, cid, ts, i, batchNbr, stagingTableSource, stagingTableTarget);
                        rot.start();
                        observerList.add(rot);
                        cst = new dbReconcile(i, "source", sqlSource, tableFilter, modColumn, parallelDegree, sourceSchema, sourceTable, ciSource.nbrColumns, ciSource.nbrPKColumns, cid, ts, ciSource.pkList, Boolean.parseBoolean(Props.getProperty("source-database-hash")), batchNbr, tid, stagingTableSource, qs);
                        cst.start();
                        compareList.add(cst);
                        ctt = new dbReconcile(i, "target", sqlTarget, tableFilter, modColumn, parallelDegree, targetSchema, targetTable, ciTarget.nbrColumns, ciTarget.nbrPKColumns, cid, ts, ciTarget.pkList, Boolean.parseBoolean(Props.getProperty("target-database-hash")), batchNbr, tid, stagingTableTarget, qt);
                        ctt.start();
                        compareList.add(ctt);
                        for (int li = 1; li <= Integer.parseInt(Props.getProperty("loader-threads")); li++) {
                            dbLoader cls = new dbLoader(i, li, "source", qs, stagingTableSource, ts);
                            cls.start();
                            loaderList.add(cls);
                            dbLoader clt = new dbLoader(i, li, "target", qt, stagingTableTarget, ts);
                            clt.start();
                            loaderList.add(clt);
                        }
                        Thread.sleep(2000);
                    }

                    Logging.write("info", "reconcile-controller", "Waiting for hash threads to complete");
                    ////////////////////////////////////////////////////////////////
                    // Check Threads
                    ////////////////////////////////////////////////////////////////
                    for (dbReconcile thread : compareList) {
                        thread.join();
                    }

                    Logging.write("info", "reconcile-controller", "Waiting for reconcile threads to complete");
                    for (dbReconcileObserver thread : observerList) {
                        thread.join();
                    }
                }
            }

            ////////////////////////////////////////
            // Summarize Results
            ////////////////////////////////////////
            dbPostgres.simpleExecute(repoConn, "set enable_nestloop='off'");

            binds.clear();
            binds.add(0, targetTable);
            binds.add(1, targetTable);

            Logging.write("info", "reconcile-controller", "Analyzing: Step 1 of 3 - Missing on Source");
            Integer missingSource = dbPostgres.simpleUpdate(repoConn, sqlMarkMissingSource, binds, true);

            Logging.write("info", "reconcile-controller", "Analyzing: Step 2 of 3 - Missing on Target");
            Integer missingTarget = dbPostgres.simpleUpdate(repoConn, sqlMarkMissingTarget, binds, true);

            Logging.write("info", "reconcile-controller", "Analyzing: Step 3 of 3 - Not Equal");
            Integer notEqual = dbPostgres.simpleUpdate(repoConn, sqlMarkNESource, binds, true);

            dbPostgres.simpleUpdate(repoConn, sqlMarkNETarget, binds, true);

            result.put("missingSource", missingSource);
            result.put("missingTarget", missingTarget);
            result.put("notEqual", notEqual);
            if (result.getString("compareStatus").equals("processing")) {
                result.put("compareStatus", (missingSource + missingTarget + notEqual > 0) ? "out-of-sync" : "in-sync");
            }

            ///////////////////////////////////////////////////////
            // Update and Check Status
            ///////////////////////////////////////////////////////
            binds.clear();
            binds.add(0, missingSource);
            binds.add(1, missingTarget);
            binds.add(2, notEqual);
            binds.add(3, result.getString("compareStatus"));
            binds.add(4, cid);
            CachedRowSet crsResult = dbPostgres.simpleUpdateReturning(repoConn, sqlUpdateStatus, binds);

            while (crsResult.next()) {
                result.put("equal", crsResult.getInt(1));
            }

            crsResult.close();

            Logging.write("info", "reconcile-controller", "Reconciliation Complete:  Table = " + targetTable + "; Equal = " + result.getInt("equal") + "; Not Equal = " + result.getInt("notEqual") + "; Missing Source = " + result.getInt("missingSource") + "; Missing Target = " + result.getInt("missingTarget"));

            result.put("status", "success");

        }  catch( SQLException e) {
            Logging.write("severe", "reconcile-controller", "Database error " + e.getMessage());
        } catch (Exception e) {
            Logging.write("severe", "reconcile-controller", "Error in reconcile controller " + e.getMessage());
        }

        return result;
    }

}
