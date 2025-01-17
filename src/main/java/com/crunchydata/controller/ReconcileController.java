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
import static com.crunchydata.util.SQLConstantsRepo.*;

import org.json.JSONObject;

/**
 * ReconcileController class that manages the data reconciliation process.
 *
 * @author Brian Pace
 */
public class ReconcileController {

    private static final String THREAD_NAME = "ReconcileController";

    private static final RepoController rpc = new RepoController();

    private static List<threadReconcile> compareList = new ArrayList<>();
    private static List<threadLoader> loaderList = new ArrayList<>();
    private static List<threadReconcileObserver> observerList = new ArrayList<>();

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

        // Variables
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject columnMap;
        Boolean useLoaderThreads = (Integer.parseInt(Props.getProperty("loader-threads")) > 0);

        BlockingQueue<DataCompare[]> qs;
        BlockingQueue<DataCompare[]> qt;
        if (useLoaderThreads) {
           qs = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
           qt = new ArrayBlockingQueue<>(Integer.parseInt(Props.getProperty("message-queue-size")));
        } else {
           qs = null;
           qt = null;
        }

        // Prepare JSON formatted results
        JSONObject result = new JSONObject();
        result.put("tableName", dct.getTableAlias());
        result.put("status", "processing");
        result.put("compareStatus", "processing");
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);

        try {
            // Get Column Info and Mapping
            binds.add(0, dct.getTid());
            String columnMapping = dbCommon.simpleSelectReturnString(connRepo, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);


            // Preflight checks
            if ( ! reconcilePreflight(dct, dctmSource, dctmTarget, columnMapping)) {
                result.put("status", "failed");
                result.put("compareStatus", "failed");
                return result;
            }

            columnMap = new JSONObject(columnMapping);

            ColumnMetadata ciSource = getColumnInfo(columnMap, "source", Props.getProperty("source-type"), dctmSource.getSchemaName(), dctmSource.getTableName(), !check && Boolean.parseBoolean(Props.getProperty("source-database-hash")));
            ColumnMetadata ciTarget = getColumnInfo(columnMap, "target", Props.getProperty("target-type"), dctmTarget.getSchemaName(), dctmTarget.getTableName(), !check && Boolean.parseBoolean(Props.getProperty("target-database-hash")));

            Logging.write("info", THREAD_NAME, String.format("Source Columns: %s", ciSource.columnList));
            Logging.write("info", THREAD_NAME, String.format("Target Columns: %s", ciTarget.columnList));
            Logging.write("info", THREAD_NAME, String.format("Source PK Columns: %s", ciSource.pkList));
            Logging.write("info", THREAD_NAME, String.format("Target PK Columns: %s", ciTarget.pkList));

            Integer cid = rpc.dcrCreate(connRepo, dctmTarget.getTid(), dctmTarget.getTableAlias(), rid);

            // Set Source & Target Variables
            // For useDatabaseHash, we do not want to use has if we are performing a recheck (check=true) or
            // use database hash has been specified for a normal compare run.
            dctmSource.setCompareSQL(switch (Props.getProperty("source-type")) {
                case "postgres" -> dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                case "oracle" -> dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                case "mariadb" -> dbMariaDB.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                case "mysql" -> dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                case "mssql" -> dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                case "db2" -> dbDB2.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("source-database-hash")), dctmSource, ciSource);
                default -> "";
            });

            dctmTarget.setCompareSQL(switch (Props.getProperty("target-type")) {
                case "postgres" -> dbPostgres.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                case "oracle" -> dbOracle.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                case "mariadb" -> dbMariaDB.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                case "mysql" -> dbMySQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                case "mssql" -> dbMSSQL.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                case "db2" -> dbDB2.buildLoadSQL(!check && Boolean.parseBoolean(Props.getProperty("target-database-hash")), dctmTarget, ciTarget);
                default -> "";
            });

            Logging.write("info", THREAD_NAME, String.format("Source Compare Hash SQL: %s", dctmSource.getCompareSQL()));
            Logging.write("info", THREAD_NAME, String.format("Target Compare Hash SQL: %s", dctmTarget.getCompareSQL()));

            if (check) {
                threadReconcileCheck.checkRows(Props, connRepo, connSource, connTarget, dct, dctmSource, dctmTarget, ciSource, ciTarget, cid);
            } else {
                // Execute Compare SQL
                if (ciTarget.pkList.isBlank() || ciTarget.pkList.isEmpty() || ciSource.pkList.isBlank() || ciSource.pkList.isEmpty()) {
                    Logging.write("warning", THREAD_NAME, String.format("Table %s has no Primary Key, skipping reconciliation",dctmTarget.getTableName()));
                    result.put("status", "skipped");
                    result.put("compareStatus", "skipped");
                    binds.add(0, cid);
                    dbCommon.simpleUpdate(connRepo, "UPDATE dc_result SET equal_cnt=0,missing_source_cnt=0,missing_target_cnt=0,not_equal_cnt=0,source_cnt=0,target_cnt=0,status='skipped' WHERE cid=?", binds, true);
                } else {
                    Logging.write("info", THREAD_NAME, "Starting compare hash threads");

                    // Start Reconciliation Threads
                    for (Integer i = 0; i < dct.getParallelDegree(); i++) {
                        Logging.write("info", THREAD_NAME, "Creating data compare staging tables");
                        String stagingTableSource = rpc.createStagingTable(Props, connRepo, "source", dct.getTid(), i);
                        String stagingTableTarget = rpc.createStagingTable(Props, connRepo, "target", dct.getTid(), i);

                        Logging.write("info", THREAD_NAME, String.format("Starting compare thread %s",i));

                        // Start Observer Thread
                        ThreadSync ts = new ThreadSync();
                        threadReconcileObserver rot = new threadReconcileObserver(Props, dct, cid, ts, i, stagingTableSource, stagingTableTarget);
                        rot.start();
                        observerList.add(rot);

                        // Start Source Reconcile Thread
                        // Reconcile threads load results into the message queue where they are saved to the database using the Loader Threads
                        threadReconcile cst = new threadReconcile(Props, i, dct, dctmSource, ciSource, cid, ts, Boolean.parseBoolean(Props.getProperty("source-database-hash")), stagingTableSource, qs);
                        cst.start();
                        compareList.add(cst);

                        // Start Target Reconcile Thread
                        // Reconcile threads load results into the message queue where they are saved to the database using the Loader Threads
                        threadReconcile ctt = new threadReconcile(Props, i, dct, dctmTarget, ciTarget, cid, ts, Boolean.parseBoolean(Props.getProperty("target-database-hash")), stagingTableTarget, qt);
                        ctt.start();
                        compareList.add(ctt);

                        // Start Loader Threads
                        // Loader thread load data from the message queue into the appropriate staging tables in the database.
                        // A loader thread is started for source and target up to the limit specified by system parameter loader-threads.
                        if (useLoaderThreads) {
                            for (int li = 1; li <= Integer.parseInt(Props.getProperty("loader-threads")); li++) {
                                threadLoader cls = new threadLoader(Props, i, li, "source", qs, stagingTableSource, ts);
                                cls.start();
                                loaderList.add(cls);
                                threadLoader clt = new threadLoader(Props, i, li, "target", qt, stagingTableTarget, ts);
                                clt.start();
                                loaderList.add(clt);
                            }
                        }

                        // Sleep to avoid flooding source and target databases with connections.
                        Thread.sleep(2000);

                    }

                    Logging.write("info", THREAD_NAME, "Waiting for compare threads to complete");
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
            dbCommon.simpleExecute(connRepo, "set enable_nestloop='off'");

            binds.clear();
            binds.add(0, dct.getTid());
            binds.add(1, dct.getTid());

            Logging.write("info", THREAD_NAME, "Analyzing: Step 1 of 3 - Missing on Source");
            Integer missingSource = dbCommon.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKMISSING, binds, true);

            Logging.write("info", THREAD_NAME, "Analyzing: Step 2 of 3 - Missing on Target");
            Integer missingTarget = dbCommon.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKMISSING, binds, true);

            Logging.write("info", THREAD_NAME, "Analyzing: Step 3 of 3 - Not Equal");
            Integer notEqual = dbCommon.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKNOTEQUAL, binds, true);

            dbCommon.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKNOTEQUAL, binds, true);

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
            CachedRowSet crsResult = dbCommon.simpleUpdateReturning(connRepo, SQL_REPO_DCRESULT_UPDATE_STATUSANDCOUNT, binds);

            while (crsResult.next()) {
                result.put("equal", crsResult.getInt(1));
            }

            crsResult.close();

            DecimalFormat formatter = new DecimalFormat("#,###");

            String msgFormat = "Reconciliation Complete: Table = %s; Status = %s; Equal = %s; Not Equal = %s; Missing Source = %s; Missing Target = %s";
            Logging.write("info", THREAD_NAME, String.format(msgFormat,dct.getTableAlias(), result.getString("compareStatus"), formatter.format(result.getInt("equal")), formatter.format(result.getInt("notEqual")), formatter.format(result.getInt("missingSource")), formatter.format(result.getInt("missingTarget"))));

            result.put("status", "success");

        }  catch( SQLException e) {
            Logging.write("severe", THREAD_NAME, String.format("Database error:  %s",e.getMessage()));
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error in reconcile controller:  %s",e.getMessage()));
        }

        return result;
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
