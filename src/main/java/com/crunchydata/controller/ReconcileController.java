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
import java.sql.SQLException;
import java.text.DecimalFormat;
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
import static com.crunchydata.util.SQLConstants.*;
import static com.crunchydata.util.Settings.Props;

import org.json.JSONObject;

/**
 * ReconcileController class that manages the data reconciliation process.
 *
 * @author Brian Pace
 */
public class ReconcileController {

    private static final String THREAD_NAME = "ReconcileController";

    /**
     * Reconciles data between source and target databases.
     *
     * @param repoConn       Connection to the repository database
     * @param sourceConn     Connection to the source database
     * @param targetConn     Connection to the target database
     * @param sourceSchema   Source schema name
     * @param sourceTable    Source table name
     * @param targetSchema   Target schema name
     * @param targetTable    Target table name
     * @param tableFilter    Filter to apply on the table
     * @param modColumn      Column used for modification tracking
     * @param parallelDegree Degree of parallelism
     * @param rid            Reconciliation ID
     * @param check          Whether to perform a check
     * @param batchNbr       Batch number
     * @param tid            Task ID
     * @param columnMapping  JSON string representing the column mapping
     * @param mapOnly        Whether to only perform column mapping
     * @return JSON object with reconciliation results
     */
    public static JSONObject reconcileData(Connection repoConn, Connection sourceConn, Connection targetConn, String sourceSchema, String sourceTable, String targetSchema, String targetTable, String tableFilter, String modColumn, Integer parallelDegree, long rid, Boolean check, Integer batchNbr, Integer tid, String columnMapping, Boolean mapOnly) {

        // Variables
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject columnMap = new JSONObject();
        List<threadReconcile> compareList = new ArrayList<>();
        List<dbLoader> loaderList = new ArrayList<>();
        List<threadReconcileObserver> observerList = new ArrayList<>();
        BlockingQueue<DataCompare[]> qs = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
        BlockingQueue<DataCompare[]> qt = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
        JSONObject result = new JSONObject();
        RepoController rpc = new RepoController();
        ThreadSync ts;

        try {
            // Get Column Info and Mapping
            result.put("tableName", targetTable);
            result.put("status", "processing");
            result.put("compareStatus", "processing");

            if (columnMapping.equals("{}") || mapOnly) {
                columnMap = getColumnMap("source", Props.getProperty("source-type"), sourceConn, sourceSchema, sourceTable, columnMap);
                columnMap = getColumnMap("target", Props.getProperty("target-type"), targetConn, targetSchema, targetTable, columnMap);
                Logging.write("info", THREAD_NAME, String.format("Saving column mapping for %s.",targetTable));
                rpc.saveColumnMap(repoConn, tid, columnMap.toString());

                if (mapOnly) {
                    result.put("status", "complete");
                    result.put("status", "map-only");
                    return result;
                }

            } else {
                columnMap = new JSONObject(columnMapping);
            }

            ColumnMetadata ciSource = getColumnInfo(columnMap, "source", Props.getProperty("source-type"), sourceSchema, sourceTable, !check && Boolean.parseBoolean(Props.getProperty("source-database-hash")));
            ColumnMetadata ciTarget = getColumnInfo(columnMap, "target", Props.getProperty("target-type"), targetSchema, targetTable, !check && Boolean.parseBoolean(Props.getProperty("target-database-hash")));

            Logging.write("info", THREAD_NAME, String.format("Source Columns: %s", ciSource.columnList));
            Logging.write("info", THREAD_NAME, String.format("Target Columns: %s", ciTarget.columnList));
            Logging.write("info", THREAD_NAME, String.format("Source PK Columns: %s", ciSource.pkList));
            Logging.write("info", THREAD_NAME, String.format("Target PK Columns: %s", ciTarget.pkList));

            Integer cid = rpc.dcrCreate(repoConn, tid, targetTable, rid);

            // Set Source & Target Variables
            String sqlSource = switch (Props.getProperty("source-type")) {
                case "postgres" -> dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "oracle" -> dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "mysql" -> dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                case "mssql" -> dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), sourceSchema, sourceTable, ciSource.pk, ciSource.pkJSON, ciSource.column, tableFilter);
                default -> "";
            };

            String sqlTarget = switch (Props.getProperty("target-type")) {
                case "postgres" -> dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "oracle" -> dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "mysql" -> dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                case "mssql" -> dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), targetSchema, targetTable, ciTarget.pk, ciTarget.pkJSON, ciTarget.column, tableFilter);
                default -> "";
            };

            Logging.write("info", THREAD_NAME, String.format("Source Compare Hash SQL: %s", sqlSource));
            Logging.write("info", THREAD_NAME, String.format("Target Compare Hash SQL: %s", sqlTarget));

            if (check) {
                threadReconcileCheck.checkRows(repoConn, sqlSource, sqlTarget, sourceConn, targetConn, tid, sourceTable, targetTable, ciSource, ciTarget, batchNbr, cid);
            } else {
                // Execute Compare SQL
                if (ciTarget.pkList.isBlank() || ciTarget.pkList.isEmpty() || ciSource.pkList.isBlank() || ciSource.pkList.isEmpty()) {
                    Logging.write("warning", THREAD_NAME, String.format("Table %s has no Primary Key, skipping reconciliation",targetTable));
                    result.put("status", "skipped");
                    result.put("compareStatus", "skipped");
                    binds.add(0, cid);
                    dbCommon.simpleUpdate(repoConn, "UPDATE dc_result SET equal_cnt=0,missing_source_cnt=0,missing_target_cnt=0,not_equal_cnt=0,source_cnt=0,target_cnt=0,status='skipped' WHERE cid=?", binds, true);
                } else {
                    Logging.write("info", THREAD_NAME, "Starting compare hash threads");

                    for (Integer i = 0; i < parallelDegree; i++) {
                        Logging.write("info", THREAD_NAME, "Creating data compare staging tables");
                        String stagingTableSource = rpc.createStagingTable(repoConn, "source", tid, i);
                        String stagingTableTarget = rpc.createStagingTable(repoConn, "target", tid, i);

                        Logging.write("info", THREAD_NAME, String.format("Starting compare thread %s",i));
                        ts = new ThreadSync();
                        threadReconcileObserver rot = new threadReconcileObserver(targetSchema, tid, targetTable, cid, ts, i, batchNbr, stagingTableSource, stagingTableTarget);
                        rot.start();
                        observerList.add(rot);
                        threadReconcile cst = new threadReconcile(i, "source", sqlSource, tableFilter, modColumn, parallelDegree, sourceSchema, sourceTable, ciSource.nbrColumns, ciSource.nbrPKColumns, cid, ts, ciSource.pkList, Boolean.parseBoolean(Props.getProperty("source-database-hash")), batchNbr, tid, stagingTableSource, qs);
                        cst.start();
                        compareList.add(cst);
                        threadReconcile ctt = new threadReconcile(i, "target", sqlTarget, tableFilter, modColumn, parallelDegree, targetSchema, targetTable, ciTarget.nbrColumns, ciTarget.nbrPKColumns, cid, ts, ciTarget.pkList, Boolean.parseBoolean(Props.getProperty("target-database-hash")), batchNbr, tid, stagingTableTarget, qt);
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
                        // Sleep to avoid flooding source and target databases with connections.
                        Thread.sleep(2000);
                    }

                    Logging.write("info", THREAD_NAME, "Waiting for hash threads to complete");
                    // Check Threads
                    for (threadReconcile thread : compareList) {
                        thread.join();
                    }

                    Logging.write("info", THREAD_NAME, "Waiting for reconcile threads to complete");
                    for (threadReconcileObserver thread : observerList) {
                        thread.join();
                    }
                }
            }

            ////////////////////////////////////////
            // Summarize Results
            ////////////////////////////////////////
            dbCommon.simpleExecute(repoConn, "set enable_nestloop='off'");

            binds.clear();
            binds.add(0, tid);
            binds.add(1, tid);

            Logging.write("info", THREAD_NAME, "Analyzing: Step 1 of 3 - Missing on Source");
            Integer missingSource = dbCommon.simpleUpdate(repoConn, SQL_REPO_DCSOURCE_MARKMISSING, binds, true);

            Logging.write("info", THREAD_NAME, "Analyzing: Step 2 of 3 - Missing on Target");
            Integer missingTarget = dbCommon.simpleUpdate(repoConn, SQL_REPO_DCTARGET_MARKMISSING, binds, true);

            Logging.write("info", THREAD_NAME, "Analyzing: Step 3 of 3 - Not Equal");
            Integer notEqual = dbCommon.simpleUpdate(repoConn, SQL_REPO_DCSOURCE_MARKNOTEQUAL, binds, true);

            dbCommon.simpleUpdate(repoConn, SQL_REPO_DCTARGET_MARKNOTEQUAL, binds, true);

            result.put("missingSource", missingSource);
            result.put("missingTarget", missingTarget);
            result.put("notEqual", notEqual);
            if (result.getString("compareStatus").equals("processing")) {
                result.put("compareStatus", (missingSource + missingTarget + notEqual > 0) ? "out-of-sync" : "in-sync");
            }

            // Update and Check Status
            binds.clear();
            binds.add(0, missingSource);
            binds.add(1, missingTarget);
            binds.add(2, notEqual);
            binds.add(3, result.getString("compareStatus"));
            binds.add(4, cid);
            CachedRowSet crsResult = dbCommon.simpleUpdateReturning(repoConn, SQL_REPO_DCRESULT_UPDATE_STATUSANDCOUNT, binds);

            while (crsResult.next()) {
                result.put("equal", crsResult.getInt(1));
            }

            crsResult.close();

            DecimalFormat formatter = new DecimalFormat("#,###");

            String msgFormat = "Reconciliation Complete: Table = %-30s; Status = %-12s; Equal = %19.19s; Not Equal = %19.19s; Missing Source = %19.19s; Missing Target = %19.19s";
            Logging.write("info", THREAD_NAME, String.format(msgFormat,targetTable, result.getString("compareStatus"), formatter.format(result.getInt("equal")), formatter.format(result.getInt("notEqual")), formatter.format(result.getInt("missingSource")), formatter.format(result.getInt("missingTarget"))));

            result.put("status", "success");

        }  catch( SQLException e) {
            Logging.write("severe", THREAD_NAME, String.format("Database error:  %s",e.getMessage()));
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error in reconcile controller:  %s",e.getMessage()));
        }

        return result;
    }

}
