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

package com.crunchydata.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Properties;

import com.crunchydata.controller.RepoController;
import com.crunchydata.models.DCTable;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import static com.crunchydata.services.dbConnection.getConnection;
import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_CLEARMATCH;
import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_DCRESULT_UPDATECNT;

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
    private Properties Props;
    private final Boolean useLoaderThreads;


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
    public threadObserver(Properties Props, DCTable dct, Integer cid, ThreadSync ts, Integer threadNbr, String stagingTableSource, String stagingTableTarget) {
        this.tid = dct.getTid();
        this.tableAlias = dct.getTableAlias();
        this.cid = cid;
        this.ts = ts;
        this.threadNbr = threadNbr;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTableSource = stagingTableSource;
        this.stagingTableTarget = stagingTableTarget;
        this.Props = Props;
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

        ArrayList<Object> binds = new ArrayList<>();
        int cntEqual = 0;
        int deltaCount = 0;
        int loaderThreads = Integer.parseInt(Props.getProperty("loader-threads"));
        DecimalFormat formatter = new DecimalFormat("#,###");
        int lastRun = 0;
        RepoController rpc = new RepoController();
        int sleepTime = 1000;

        // Connect to Repository
        Logging.write("info", threadName, "Connecting to repository database");
        Connection repoConn = getConnection("postgres", "repo");

        if ( repoConn == null) {
            Logging.write("severe", threadName, "Cannot connect to repository database");
            System.exit(1);
        }

        try { repoConn.setAutoCommit(false); } catch (Exception e) {
            // do nothing
        }

        try {
            dbCommon.simpleExecute(repoConn,"set enable_nestloop='off'");
            dbCommon.simpleExecute(repoConn,"set work_mem='512MB'");
            dbCommon.simpleExecute(repoConn,"set maintenance_work_mem='1024MB'");
        } catch (Exception e) {
            // do nothing
        }

        // Watch Reconcile Loop
        try {
            String sqlClearMatch = SQL_REPO_CLEARMATCH.replaceAll("dc_target",stagingTableTarget).replaceAll("dc_source",stagingTableSource);

            PreparedStatement stmtSU = repoConn.prepareStatement(sqlClearMatch);
            PreparedStatement stmtSUS = repoConn.prepareStatement(SQL_REPO_DCRESULT_UPDATECNT);

            repoConn.setAutoCommit(false);

            int tmpRowCount;

            while (lastRun <= 1) {
                // Remove Matching Rows
                tmpRowCount = stmtSU.executeUpdate();

                cntEqual += tmpRowCount;

                if (tmpRowCount > 0) {
                    repoConn.commit();
                    deltaCount += tmpRowCount;
                    Logging.write("info", threadName, String.format("Matched %s rows", formatter.format(tmpRowCount)));
                } else {
                    if (cntEqual > 0 || ts.sourceComplete || ts.targetComplete || ( cntEqual == 0 && ts.sourceWaiting && ts.targetWaiting ) ) {
                        stmtSUS.clearParameters();
                        stmtSUS.setInt(1,deltaCount);
                        stmtSUS.setInt(2,cid);
                        stmtSUS.executeUpdate();
                        repoConn.commit();
                        deltaCount=0;
                        ts.observerNotify();
                        if ( Boolean.parseBoolean(Props.getProperty("observer-vacuum")) ) {
                            repoConn.setAutoCommit(true);
                            binds.clear();
                            dbCommon.simpleUpdate(repoConn, String.format("vacuum %s,%s", stagingTableSource, stagingTableTarget), binds, false);
                            repoConn.setAutoCommit(false);
                        }
                    }
                }

                // Update and Check Status
                if ( ts.sourceComplete && ts.targetComplete && tmpRowCount == 0 && (ts.loaderThreadComplete == loaderThreads*2 || ! useLoaderThreads) ) {
                    lastRun++;
                }

                if ( tmpRowCount == 0 ) {
                    if (Props.getProperty("database-sort").equals("false") && cntEqual == 0) { ts.observerNotify(); }
                    Thread.sleep(sleepTime);
                } else {
                    // Standard Sleep
                    if ( cntEqual > 500000 ) {
                        Thread.sleep(500);
                    }
                }
            }

            stmtSUS.close();
            stmtSU.close();

            Logging.write("info", threadName, "Staging table cleanup");

            // Move Out-of-Sync rows from temporary staging tables to dc_source and dc_target
            rpc.loadFindings(repoConn, "source", tid, tableAlias, stagingTableSource, batchNbr, threadNbr);
            rpc.loadFindings(repoConn, "target", tid, tableAlias, stagingTableTarget, batchNbr, threadNbr);

            // Drop staging tables
            rpc.dropStagingTable(repoConn, stagingTableSource);
            rpc.dropStagingTable(repoConn, stagingTableTarget);


        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", threadName, String.format("Error in observer process at line %s: %s", stackTrace[0].getLineNumber(), e.getMessage()));
            try { repoConn.rollback();
            } catch (Exception ee) {
                stackTrace = e.getStackTrace();
                Logging.write("warn", threadName, String.format("Error rolling back transaction at line %s: %s ",stackTrace[0].getLineNumber(), e.getMessage()));
            }
        } finally {
            try {
                repoConn.close();
            } catch (Exception e) {
                StackTraceElement[] stackTrace = e.getStackTrace();
                Logging.write("warn", threadName, String.format("Error closing thread at line %s:  %s",stackTrace[0].getLineNumber(), e.getMessage()));
            }
        }
    }

}
