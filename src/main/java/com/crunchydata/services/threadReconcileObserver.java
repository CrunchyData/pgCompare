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

import com.crunchydata.controller.RepoController;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import static com.crunchydata.util.SQLConstants.SQL_REPO_CLEARMATCH;
import static com.crunchydata.util.SQLConstants.SQL_REPO_DCRESULT_UPDATECNT;
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
public class threadReconcileObserver extends Thread  {

    private Integer tid;
    private String tableName;
    private Integer cid;
    private Integer threadNbr;
    private Integer batchNbr;
    private String stagingTableSource;
    private String stagingTableTarget;

    private ThreadSync ts;

    /**
     * Constructs a thread to observe the reconciliation process.
     *
     * @param tid                Identifier for table.
     * @param schemaName         Schema name of the tables being reconciled.
     * @param tableName          Name of the table being reconciled.
     * @param cid                Identifier for the reconciliation process.
     * @param ts                 Thread synchronization object for coordinating threads.
     * @param threadNbr          Thread number identifier.
     * @param batchNbr           Batch number identifier.
     * @param stagingTableSource Staging table name for the source data.
     * @param stagingTableTarget Staging table name for the target data.
     *
     * @author Brian Pace
     */
    public threadReconcileObserver(String schemaName, Integer tid, String tableName, Integer cid, ThreadSync ts, Integer threadNbr, Integer batchNbr, String stagingTableSource, String stagingTableTarget) {
        this.tid = tid;
        this.tableName = tableName;
        this.cid = cid;
        this.ts = ts;
        this.threadNbr = threadNbr;
        this.batchNbr = batchNbr;
        this.stagingTableSource = stagingTableSource;
        this.stagingTableTarget = stagingTableTarget;
    }

    /**
     * Executes the reconciliation observer thread.
     * This method manages database connections, executes SQL statements for reconciliation,
     * and performs cleanup operations on staging tables.
     */
    public void run() {
        String threadName = String.format("Observer-c%s-t%s", cid, threadNbr);
        Logging.write("info", threadName, "Starting reconcile observer");

        ArrayList<Object> binds = new ArrayList<>();
        int cntEqual = 0;
        int deltaCount = 0;
        DecimalFormat formatter = new DecimalFormat("#,###");
        int lastRun = 0;
        RepoController rpc = new RepoController();
        int sleepTime = 2000;

        // Connect to Repository
        Logging.write("info", threadName, "Connecting to repository database");
        Connection repoConn = dbPostgres.getConnection(Props,"repo", "observer");

        if ( repoConn == null) {
            Logging.write("severe", threadName, "Cannot connect to repository database");
            System.exit(1);
        }

        try { repoConn.setAutoCommit(false); } catch (Exception e) {
            // do nothing
        }

        try { dbCommon.simpleExecute(repoConn,"set enable_nestloop='off'"); } catch (Exception e) {
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
                if ( ts.sourceComplete && ts.targetComplete && tmpRowCount == 0 && ts.loaderThreadComplete == Integer.parseInt(Props.getProperty("loader-threads"))*2 ) {
                    lastRun++;
                }

                if ( tmpRowCount == 0 ) {
                    if (Props.getProperty("database-sort").equals("false") && cntEqual == 0) { ts.observerNotify(); }
                    Thread.sleep(sleepTime);
                }
            }

            stmtSUS.close();
            stmtSU.close();

            Logging.write("info", threadName, "Staging table cleanup");

            rpc.loadFindings(repoConn, "source", tid, stagingTableSource, tableName, batchNbr, threadNbr);
            rpc.loadFindings(repoConn, "target", tid, stagingTableTarget, tableName, batchNbr, threadNbr);
            rpc.dropStagingTable(repoConn, stagingTableSource);
            rpc.dropStagingTable(repoConn, stagingTableTarget);


        } catch (Exception e) {
            Logging.write("severe", threadName, "Error in observer process: " + e.getMessage());
            try { repoConn.rollback();
            } catch (Exception ee) {
                Logging.write("warn", threadName, "Error rolling back transaction " + e.getMessage());
            }
        } finally {
            try {
                if (!(repoConn == null)) {
                    repoConn.close();
                }
            } catch (Exception e) {
                Logging.write("warn", threadName, "Error closing thread " + e.getMessage());
            }
        }
    }

}
