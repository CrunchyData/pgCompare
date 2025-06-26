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
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.sql.rowset.CachedRowSet;

import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.models.DataCompare;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;
import com.crunchydata.services.*;

import static com.crunchydata.controller.ColumnController.getColumnInfo;
import static com.crunchydata.services.DatabaseService.buildLoadSQL;
import static com.crunchydata.util.SQLConstantsRepo.*;
import static java.lang.Integer.parseInt;

import org.json.JSONObject;

/**
 * CompareController class that manages the data reconciliation process.
 *
 * @author Brian Pace
 */
public class CompareController {

    private static final String THREAD_NAME = "compare-ctrl";

    private static final RepoController rpc = new RepoController();
    private static final List<threadCompare> compareList = new ArrayList<>();
    private static final List<threadLoader> loaderList = new ArrayList<>();
    private static final List<threadObserver> observerList = new ArrayList<>();

    /**
     * Reconciles data between source and target databases.
     *
     * @param connRepo       Connection to the repository database
     * @param connSource     Connection to the source database
     * @param connTarget     Connection to the target database
     * @param rid            Reconciliation ID
     * @param check          Whether to perform a check
     * @return JSON object with reconciliation results
     */
    public static JSONObject reconcileData(Properties Props, Connection connRepo, Connection connSource, Connection connTarget, long rid, Boolean check, DCTable dct, DCTableMap dctmSource, DCTableMap dctmTarget) {

        ArrayList<Object> binds = new ArrayList<>();
        JSONObject columnMap;

        boolean useLoaderThreads = (parseInt(Props.getProperty("loader-threads")) > 0);
        int messageQueueSize = Integer.parseInt(Props.getProperty("message-queue-size"));

        BlockingQueue<DataCompare[]> qs = useLoaderThreads ? new ArrayBlockingQueue<>(messageQueueSize) : null;
        BlockingQueue<DataCompare[]> qt = useLoaderThreads ? new ArrayBlockingQueue<>(messageQueueSize) : null;

        // Capture the start time for the compare run.
        long startTime = System.currentTimeMillis();

        // Prepare JSON formatted results
        JSONObject checkResult;
        JSONObject result = initializeResult(dct);

        try {
            // Get Column Info and Mapping
            binds.clear();
            binds.addFirst(dct.getTid());
            String columnMapping = SQLService.simpleSelectReturnString(connRepo, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);

            // Preflight checks
            if ( ! reconcilePreflight(dct, dctmSource, dctmTarget, columnMapping)) {
                result.put("status", "failed");
                result.put("compareStatus", "failed");
                return result;
            }

            // Get column mapping and metadata
            columnMap = new JSONObject(columnMapping);
            ColumnMetadata ciSource = getColumnInfo(columnMap, "source", Props.getProperty("source-type"), dctmSource.getSchemaName(), dctmSource.getTableName(), !check && "database".equals(Props.getProperty("column-hash-method")));
            ColumnMetadata ciTarget = getColumnInfo(columnMap, "target", Props.getProperty("target-type"), dctmTarget.getSchemaName(), dctmTarget.getTableName(), !check && "database".equals(Props.getProperty("column-hash-method")));
            logColumnMetadata(ciSource, ciTarget);

            // Create Compare ID (cid) for this run
            Integer cid = rpc.dcrCreate(connRepo, dctmTarget.getTid(), dctmTarget.getTableAlias(), rid);

            // Generate Compare SQL
            prepareCompareSQL(Props, dctmSource, dctmTarget, ciSource, ciTarget);

            if (check) {
                // Execute recheck of rows that were out of sync during last run
                checkResult = threadCheck.checkRows(Props, connRepo, connSource, connTarget, dct, dctmSource, dctmTarget, ciSource, ciTarget, cid);
                result.put("checkResult", checkResult);
            } else {
                // Execute Compare SQL
                if (ciTarget.pkList.isBlank() || ciTarget.pkList.isEmpty() || ciSource.pkList.isBlank() || ciSource.pkList.isEmpty()) {
                    // If there is no Primary Key, skipp compare
                    skipReconciliation(connRepo, result, dctmTarget.getTableName(), cid);
                } else {
                    Logging.write("info", THREAD_NAME, "Starting compare hash threads");
                    startReconcileThreads(Props, dct, cid, dctmSource, dctmTarget, ciSource, ciTarget, qs, qt, useLoaderThreads, connRepo);

                    Logging.write("info", THREAD_NAME, "Waiting for compare threads to complete");
                    joinThreads(compareList);

                    Logging.write("info", THREAD_NAME, "Waiting for reconcile threads to complete");
                    joinThreads(observerList);
                }
            }

            // Summarize Results
            summarizeResults(connRepo, dct.getTid(), result, cid);
            long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;

            result.put("elapsedTime", elapsedTime);
            result.put("rowsPerSecond", (result.getInt("elapsedTime") > 0 ) ? result.getInt("totalRows")/elapsedTime : result.getInt("totalRows"));

            logFinalResult(result, dct.getTableAlias());

            result.put("status", "success");

        }  catch( SQLException e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            result.put("status", "failed");
            Logging.write("severe", THREAD_NAME, String.format("Database error at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            result.put("status", "failed");
            Logging.write("severe", THREAD_NAME, String.format("Error in reconcile controller at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        return result;
    }

    private static JSONObject initializeResult(DCTable dct) {
        JSONObject result = new JSONObject();
        result.put("tableName", dct.getTableAlias());
        result.put("status", "processing");
        result.put("compareStatus", "processing");
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);
        return result;
    }

    private static void joinThreads(List<? extends Thread> threads) throws InterruptedException {
        for (Thread t : threads) {
            t.join();
        }
    }

    private static void logColumnMetadata(ColumnMetadata source, ColumnMetadata target) {
        Logging.write("info", THREAD_NAME, "(source) Columns: " + source.columnList);
        Logging.write("info", THREAD_NAME, "(target) Columns: " + target.columnList);
        Logging.write("info", THREAD_NAME, "(source) PK Columns: " + source.pkList);
        Logging.write("info", THREAD_NAME, "(target) PK Columns: " + target.pkList);
    }

    private static void logFinalResult(JSONObject result, String tableAlias) {
        DecimalFormat formatter = new DecimalFormat("#,###");
        Logging.write("info", THREAD_NAME, String.format(
                "Reconciliation Complete: Table = %s; Status = %s; Equal = %s; Not Equal = %s; Missing Source = %s; Missing Target = %s",
                tableAlias, result.getString("compareStatus"),
                formatter.format(result.getInt("equal")),
                formatter.format(result.getInt("notEqual")),
                formatter.format(result.getInt("missingSource")),
                formatter.format(result.getInt("missingTarget"))
        ));
    }

    private static void prepareCompareSQL(Properties props, DCTableMap source, DCTableMap target,
                                          ColumnMetadata ciSource, ColumnMetadata ciTarget) {
        String method = props.getProperty("column-hash-method");
        source.setCompareSQL(buildLoadSQL(method, source, ciSource));
        target.setCompareSQL(buildLoadSQL(method, target, ciTarget));

        Logging.write("info", THREAD_NAME, "(source) Compare SQL: " + source.getCompareSQL());
        Logging.write("info", THREAD_NAME, "(target) Compare SQL: " + target.getCompareSQL());
    }

    private static void skipReconciliation(Connection connRepo, JSONObject result, String tableName, int cid) {
        Logging.write("warning", THREAD_NAME, String.format("Table %s has no Primary Key, skipping reconciliation", tableName));
        result.put("status", "skipped");
        result.put("compareStatus", "skipped");

        ArrayList<Object> binds = new ArrayList<>();
        binds.addFirst(cid);
        SQLService.simpleUpdate(connRepo, "UPDATE dc_result SET equal_cnt=0,missing_source_cnt=0,missing_target_cnt=0,not_equal_cnt=0,source_cnt=0,target_cnt=0,status='skipped' WHERE cid=?", binds, true);
    }

    private static void startReconcileThreads(Properties props, DCTable dct, int cid, DCTableMap dctmSource, DCTableMap dctmTarget,
                                              ColumnMetadata ciSource, ColumnMetadata ciTarget,
                                              BlockingQueue<DataCompare[]> qs, BlockingQueue<DataCompare[]> qt,
                                              boolean useLoaderThreads,
                                              Connection connRepo) throws InterruptedException {

        for (int i = 0; i < dct.getParallelDegree(); i++) {
            ThreadSync ts = new ThreadSync();

            String columnHashMethod = props.getProperty("column-hash-method");

            String stagingSource = rpc.createStagingTable(props, connRepo, "source", dct.getTid(), i);
            String stagingTarget = rpc.createStagingTable(props, connRepo, "target", dct.getTid(), i);

            threadObserver observer = new threadObserver(props, dct, cid, ts, i, stagingSource, stagingTarget);
            observer.start();
            observerList.add(observer);

            threadCompare srcThread = new threadCompare(props, i, dct, dctmSource, ciSource, cid, ts,
                    columnHashMethod.equals("database"), stagingSource, qs);
            threadCompare tgtThread = new threadCompare(props, i, dct, dctmTarget, ciTarget, cid, ts,
                    columnHashMethod.equals("database"), stagingTarget, qt);

            srcThread.start(); compareList.add(srcThread);
            tgtThread.start(); compareList.add(tgtThread);

            if (useLoaderThreads) {
                int loaderThreads = Integer.parseInt(props.getProperty("loader-threads"));
                for (int li = 1; li <= loaderThreads; li++) {
                    threadLoader loaderSrc = new threadLoader(props, i, li, "source", qs, stagingSource, ts);
                    threadLoader loaderTgt = new threadLoader(props, i, li, "target", qt, stagingTarget, ts);
                    loaderSrc.start(); loaderList.add(loaderSrc);
                    loaderTgt.start(); loaderList.add(loaderTgt);
                }
            }

            Thread.sleep(2000);
        }
    }

    private static void summarizeResults(Connection connRepo, long tid, JSONObject result, int cid) throws SQLException {
        SQLService.simpleExecute(connRepo, "set enable_nestloop='off'");

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, tid);
        binds.add(1, tid);
        int missingSource = SQLService.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKMISSING,  binds, true);
        int missingTarget = SQLService.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKMISSING, binds, true);
        int notEqual = SQLService.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKNOTEQUAL, binds, true);
        SQLService.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKNOTEQUAL, binds, true);

        result.put("missingSource", missingSource);
        result.put("missingTarget", missingTarget);
        result.put("notEqual", notEqual);

        if ("processing".equals(result.getString("compareStatus"))) {
            result.put("compareStatus", (missingSource + missingTarget + notEqual > 0) ? "out-of-sync" : "in-sync");
        }

        binds.clear();
        binds.add(0, missingSource);
        binds.add(1, missingTarget);
        binds.add(2, notEqual);
        binds.add(3, result.getString("compareStatus"));
        binds.add(4, cid);

        try (CachedRowSet crs = SQLService.simpleUpdateReturning(connRepo, SQL_REPO_DCRESULT_UPDATE_STATUSANDCOUNT, binds)) {
            if (crs.next()) {
                int equal = crs.getInt(1);
                result.put("equal", equal);
                result.put("totalRows", equal + missingSource + missingTarget + notEqual);
            }
        }
    }

    private static Boolean reconcilePreflight(DCTable dct, DCTableMap dctmSource, DCTableMap dctmTarget, String columnMapping) {
        // Ensure target and source have mod_column if parallel_degree > 1
        if ( dct.getParallelDegree() > 1 && dctmSource.getModColumn().isEmpty() && dctmTarget.getModColumn().isEmpty() ) {
            Logging.write("severe",THREAD_NAME, String.format("Parallel degree is greater than 1 for table %s, but no value specified for mod_column on source and/or target.",dct.getTableAlias()));
            return false;
        }

        // Verify column mapping exists
        if (columnMapping == null) {
            Logging.write("severe",THREAD_NAME, String.format("No column map found for table %s.  Consider running with maponly option to create mappings.",dct.getTableAlias()));
            return false;
        }

        return true;
    }

}
