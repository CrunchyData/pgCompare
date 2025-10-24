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

package com.crunchydata.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import com.crunchydata.controller.RepoController;
import com.crunchydata.model.DCTable;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import static com.crunchydata.service.dbConnection.getConnection;
import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_CLEARMATCH;
import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_DCRESULT_UPDATECNT;
import static com.crunchydata.util.Settings.Props;

/**
 * Thread class that observes the reconciliation process between source and target tables.
 * This thread executes SQL statements to manage reconciliation and cleanup of staging tables.
 * <p>
 * The observer notifies synchronization threads upon completion of reconciliation steps.
 * </p>
 * <p>
 * Configuration settings include database connection details and SQL statements for reconciliation.
 * </p>
 * <p>
 * This class extends Thread and is designed to run independently for reconciliation monitoring.
 * </p>
 *
 * @author Brian Pace
 */
public class threadObserver extends Thread  {

    private final Integer tid;
    private final String tableAlias;
    private final Integer cid;
    private final Integer threadNbr;
    private final Integer batchNbr;
    private final String stagingTableSource;
    private final String stagingTableTarget;
    private ThreadSync ts;
    private final Boolean useLoaderThreads;
    
    // Constants for better maintainability
    private static final int DEFAULT_SLEEP_TIME_MS = 1000;
    private static final int HIGH_VOLUME_SLEEP_TIME_MS = 500;
    private static final int HIGH_VOLUME_THRESHOLD = 500000;
    private static final int MAX_LAST_RUN_COUNT = 1;
    private static final String POSTGRES_OPTIMIZATION_NESTLOOP = "set enable_nestloop='off'";
    private static final String POSTGRES_OPTIMIZATION_WORK_MEM = "set work_mem='512MB'";
    private static final String POSTGRES_OPTIMIZATION_MAINTENANCE_WORK_MEM = "set maintenance_work_mem='1024MB'";


    /**
     * Constructs a thread to observe the reconciliation process.
     *
     * @param cid                Identifier for the reconciliation process.
     * @param ts                 Thread synchronization object for coordinating threads.
     * @param threadNbr          Thread number identifier.
     * @param stagingTableSource Staging table name for the source data.
     * @param stagingTableTarget Staging table name for the target data.
     *
     * @author Brian Pace
     */
    public threadObserver(DCTable dct, Integer cid, ThreadSync ts, Integer threadNbr, String stagingTableSource, String stagingTableTarget) {
        this.tid = dct.getTid();
        this.tableAlias = dct.getTableAlias();
        this.cid = cid;
        this.ts = ts;
        this.threadNbr = threadNbr;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTableSource = stagingTableSource;
        this.stagingTableTarget = stagingTableTarget;
        this.useLoaderThreads =  (Integer.parseInt(Props.getProperty("loader-threads")) > 0);
    }

    /**
     * Executes the reconciliation observer thread.
     * This method manages database connections, executes SQL statements for reconciliation,
     * and performs cleanup operations on staging tables.
     */
    public void run() {
        String threadName = String.format("observer-c%s-t%s", cid, threadNbr);
        Logging.write("info", threadName, "Starting reconcile observer");

        // Configuration variables
        ArrayList<Object> binds = new ArrayList<>();
        int cntEqual = 0;
        int deltaCount = 0;
        int loaderThreads = Integer.parseInt(Props.getProperty("loader-threads"));
        DecimalFormat formatter = new DecimalFormat("#,###");
        int lastRun = 0;
        RepoController rpc = new RepoController();
        int sleepTime = DEFAULT_SLEEP_TIME_MS;

        Connection repoConn = null;
        PreparedStatement stmtSU = null;
        PreparedStatement stmtSUS = null;

        try {
            // Initialize repository connection
            repoConn = initializeRepositoryConnection(threadName);
            
            // Execute main reconciliation observer logic
            executeReconciliationObserver(threadName, repoConn, binds, cntEqual, deltaCount, loaderThreads, 
                                        formatter, lastRun, rpc, sleepTime);

        } catch (Exception e) {
            Logging.write("severe", threadName, String.format("Error in observer process: %s", e.getMessage()));
            performRollback(threadName, repoConn);
        } finally {
            // Clean up resources
            cleanupResources(threadName, stmtSU, stmtSUS, repoConn);
        }
    }
    
    /**
     * Initializes repository connection with proper error handling.
     */
    private Connection initializeRepositoryConnection(String threadName) throws Exception {
        Logging.write("info", threadName, "Connecting to repository database");
        Connection repoConn = getConnection("postgres", "repo");

        if (repoConn == null) {
            throw new Exception("Cannot connect to repository database");
        }

        try {
            repoConn.setAutoCommit(false);
        } catch (Exception e) {
            // Auto-commit setting is not critical, continue
        }

        // Apply PostgreSQL optimizations
        try {
            SQLService.simpleExecute(repoConn, POSTGRES_OPTIMIZATION_NESTLOOP);
            SQLService.simpleExecute(repoConn, POSTGRES_OPTIMIZATION_WORK_MEM);
            SQLService.simpleExecute(repoConn, POSTGRES_OPTIMIZATION_MAINTENANCE_WORK_MEM);
        } catch (Exception e) {
            // Optimizations are not critical, continue
        }
        
        return repoConn;
    }
    
    /**
     * Executes the main reconciliation observer logic.
     */
    private void executeReconciliationObserver(String threadName, Connection repoConn, ArrayList<Object> binds,
                                             int cntEqual, int deltaCount, int loaderThreads, DecimalFormat formatter,
                                             int lastRun, RepoController rpc, int sleepTime) throws Exception {
        String sqlClearMatch = SQL_REPO_CLEARMATCH.replaceAll("dc_target", stagingTableTarget)
                                                 .replaceAll("dc_source", stagingTableSource);

        try (PreparedStatement stmtSU = repoConn.prepareStatement(sqlClearMatch);
             PreparedStatement stmtSUS = repoConn.prepareStatement(SQL_REPO_DCRESULT_UPDATECNT)) {

            repoConn.setAutoCommit(false);
            int tmpRowCount;

            while (lastRun <= MAX_LAST_RUN_COUNT) {
                // Remove matching rows
                tmpRowCount = stmtSU.executeUpdate();
                cntEqual += tmpRowCount;

                if (tmpRowCount > 0) {
                    repoConn.commit();
                    deltaCount += tmpRowCount;
                    Logging.write("info", threadName, String.format("Matched %s rows", formatter.format(tmpRowCount)));
                } else {
                    handleNoMatches(cntEqual, deltaCount, binds, rpc, repoConn, stmtSUS);
                }

                // Update and check status
                if (isReconciliationComplete(tmpRowCount, loaderThreads)) {
                    lastRun++;
                }

                // Handle sleep timing
                handleSleepTiming(tmpRowCount, cntEqual, sleepTime);
            }

            // Perform cleanup operations
            performCleanup(threadName, repoConn, rpc);
        }
    }
    
    /**
     * Handles the case when no matches are found.
     */
    private void handleNoMatches(int cntEqual, int deltaCount, ArrayList<Object> binds, RepoController rpc,
                                Connection repoConn, PreparedStatement stmtSUS) throws Exception {
        if (cntEqual > 0 || ts.sourceComplete || ts.targetComplete || 
            (cntEqual == 0 && ts.sourceWaiting && ts.targetWaiting)) {
            
            // Update result counts
            stmtSUS.clearParameters();
            stmtSUS.setInt(1, deltaCount);
            stmtSUS.setInt(2, cid);
            stmtSUS.executeUpdate();
            repoConn.commit();
            deltaCount = 0;
            ts.observerNotify();
            
            // Handle vacuum if enabled
            if (Boolean.parseBoolean(Props.getProperty("observer-vacuum", "false"))) {
                performVacuum(binds, repoConn);
            }
        }
    }
    
    /**
     * Performs vacuum operation on staging tables.
     */
    private void performVacuum(ArrayList<Object> binds, Connection repoConn) throws Exception {
        repoConn.setAutoCommit(true);
        binds.clear();
        SQLService.simpleUpdate(repoConn, 
            String.format("vacuum %s,%s", stagingTableSource, stagingTableTarget), binds, false);
        repoConn.setAutoCommit(false);
    }
    
    /**
     * Checks if reconciliation is complete.
     */
    private boolean isReconciliationComplete(int tmpRowCount, int loaderThreads) {
        return ts.sourceComplete && ts.targetComplete && tmpRowCount == 0 && 
               (ts.loaderThreadComplete == loaderThreads * 2 || !useLoaderThreads);
    }
    
    /**
     * Handles sleep timing based on processing volume.
     */
    private void handleSleepTiming(int tmpRowCount, int cntEqual, int sleepTime) throws InterruptedException {
        if (tmpRowCount == 0) {
            if (Props.getProperty("database-sort", "true").equals("false") && cntEqual == 0) {
                ts.observerNotify();
            }
            Thread.sleep(sleepTime);
        } else {
            // Standard sleep with high volume optimization
            if (cntEqual > HIGH_VOLUME_THRESHOLD) {
                Thread.sleep(HIGH_VOLUME_SLEEP_TIME_MS);
            }
        }
    }
    
    /**
     * Performs cleanup operations including loading findings and dropping staging tables.
     */
    private void performCleanup(String threadName, Connection repoConn, RepoController rpc) throws Exception {
        Logging.write("info", threadName, "Staging table cleanup");

        // Move out-of-sync rows from temporary staging tables to dc_source and dc_target
        rpc.loadFindings(repoConn, "source", tid, tableAlias, stagingTableSource, batchNbr, threadNbr);
        rpc.loadFindings(repoConn, "target", tid, tableAlias, stagingTableTarget, batchNbr, threadNbr);

        // Drop staging tables
        rpc.dropStagingTable(repoConn, stagingTableSource);
        rpc.dropStagingTable(repoConn, stagingTableTarget);
    }
    
    /**
     * Performs transaction rollback.
     */
    private void performRollback(String threadName, Connection repoConn) {
        if (repoConn != null) {
            try {
                repoConn.rollback();
            } catch (Exception e) {
                Logging.write("warning", threadName, String.format("Error rolling back transaction: %s", e.getMessage()));
            }
        }
    }
    
    /**
     * Cleans up database resources.
     */
    private void cleanupResources(String threadName, PreparedStatement stmtSU, PreparedStatement stmtSUS, Connection repoConn) {
        try {
            if (stmtSU != null) {
                stmtSU.close();
            }
            if (stmtSUS != null) {
                stmtSUS.close();
            }
            if (repoConn != null) {
                repoConn.close();
            }
        } catch (Exception e) {
            Logging.write("warning", threadName, String.format("Error closing resources: %s", e.getMessage()));
        }
    }

}
