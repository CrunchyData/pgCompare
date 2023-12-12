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

package com.crunchydata.services;

import com.crunchydata.controller.RepoController;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

import static com.crunchydata.util.Settings.Props;

/**
 * @author Brian Pace
 */
public class dbReconcileObserver extends Thread  {
    String schemaName;
    String tableName;
    Integer cid;
    String threadName;
    Integer threadNbr;
    Integer batchNbr;
    String stagingTableSource;
    String stagingTableTarget;

    ThreadSync ts;

    /////////////////////////////////////////////////
    // Configuration Settings
    /////////////////////////////////////////////////

    public dbReconcileObserver(String schemaName, String tableName, Integer cid, ThreadSync ts, Integer threadNbr, Integer batchNbr, String stagingTableSource, String stagingTableTarget) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.cid = cid;
        this.ts = ts;
        this.threadNbr = threadNbr;
        this.batchNbr = batchNbr;
        this.stagingTableSource = stagingTableSource;
        this.stagingTableTarget = stagingTableTarget;
    }

    public void run() {
        RepoController rpc = new RepoController();

        ArrayList<Object> binds = new ArrayList<>();
        int cntEqual = 0;

        int lastRun = 0;
        int sleepTime = 2000;
        int deltaCount = 0;

        threadName = "observer-"+cid+"-"+threadNbr;
        Logging.write("info", threadName, "Starting reconcile observer");

        /////////////////////////////////////////////////
        // Connect to Repository
        /////////////////////////////////////////////////
        Connection repoConn;
        Logging.write("info", threadName, "Connecting to repository database");
        repoConn = dbPostgres.getConnection(Props,"repo", "observer");
        if ( repoConn == null) {
            Logging.write("severe", threadName, "Cannot connect to repository database");
            System.exit(1);
        }
        try { repoConn.setAutoCommit(false); } catch (Exception e) {
            // do nothing
        }
        try { dbPostgres.simpleExecute(repoConn,"set enable_nestloop='off'"); } catch (Exception e) {
            // do nothing
        }

        /////////////////////////////////////////////////
        // Watch Reconcile Loop
        /////////////////////////////////////////////////

        String sqlClearMatch = """
                WITH ds AS (DELETE FROM dc_source s
                            WHERE EXISTS
                                      (SELECT 1
                                       FROM dc_target t
                                       WHERE table_name=?
                                             AND s.pk_hash = t.pk_hash
                                             AND s.column_hash = t.column_hash
                                             AND s.thread_nbr = t.thread_nbr
                                             AND s.batch_nbr = t.batch_nbr)
                                   AND table_name=?
                                   AND thread_nbr=?
                                   AND batch_nbr=?
                            RETURNING table_name, pk_hash, column_hash, thread_nbr, batch_nbr)
                DELETE FROM dc_target dt USING ds
                WHERE ds.table_name=dt.table_name
                       AND ds.pk_hash=dt.pk_hash
                       AND ds.column_hash=dt.column_hash
                       AND ds.thread_nbr=dt.thread_nbr
                       AND ds.batch_nbr=dt.batch_nbr
                       AND dt.thread_nbr=?
                       AND dt.batch_nbr=?
                """;

        String sqlUpdateStatus = """
                                 UPDATE dc_result SET equal_cnt=equal_cnt+?
                                 WHERE cid=?
                                 """;

        try {
            sqlClearMatch = sqlClearMatch.replaceAll("dc_target",stagingTableTarget).replaceAll("dc_source",stagingTableSource);

            PreparedStatement stmtSU = repoConn.prepareStatement(sqlClearMatch);

            repoConn.setAutoCommit(false);

            int tmpRowCount;

            while (lastRun <= 1) {
                ///////////////////////////////////////////////////////
                // Remove Matching Rows
                ///////////////////////////////////////////////////////

                stmtSU.setObject(1, tableName);
                stmtSU.setObject(2, tableName);
                stmtSU.setInt(3, threadNbr);
                stmtSU.setInt(4, batchNbr);
                stmtSU.setInt(5, threadNbr);
                stmtSU.setInt(6, batchNbr);
                tmpRowCount = stmtSU.executeUpdate();

                cntEqual = cntEqual + tmpRowCount;

                if (tmpRowCount > 0) {
                    repoConn.commit();
                    deltaCount += tmpRowCount;
                    Logging.write("info", threadName, "Matched " + tmpRowCount + " rows");
                } else {
                    if (cntEqual > 0 || ts.sourceComplete || ts.targetComplete ) {
                        ts.ObserverNotify();
                        binds.clear();
                        binds.add(0,deltaCount);
                        binds.add(1, cid);
                        dbPostgres.simpleUpdate(repoConn, sqlUpdateStatus, binds, true);
                        deltaCount=0;
                        if ( Boolean.parseBoolean(Props.getProperty("observer-vacuum")) ) {
                            repoConn.setAutoCommit(true);
                            binds.clear();
                            dbPostgres.simpleUpdate(repoConn, "vacuum " + stagingTableSource + "," + stagingTableTarget, binds, false);
                            repoConn.setAutoCommit(false);
                        }
                    }
                }

                ///////////////////////////////////////////////////////
                // Update and Check Status
                ///////////////////////////////////////////////////////
                if (ts.sourceComplete && ts.targetComplete && tmpRowCount == 0) {
                    lastRun++;
                }

                if ( tmpRowCount == 0 ) {
                    if (Props.getProperty("database-sort").equals("false") && cntEqual == 0) { ts.ObserverNotify(); }
                    Thread.sleep(sleepTime);
                }
            }

            stmtSU.close();

            Logging.write("info", threadName, "Staging table cleanup");

            rpc.loadFindings(repoConn, "source", stagingTableSource);
            rpc.loadFindings(repoConn, "target", stagingTableTarget);
            rpc.dropStagingTable(repoConn, stagingTableSource);
            rpc.dropStagingTable(repoConn, stagingTableTarget);


        } catch (Exception e) {
            Logging.write("severe", threadName, "Error in observer process: " + e.getMessage());
            try { repoConn.rollback(); } catch (Exception ee) {
                // do nothing
            }
            try { repoConn.close(); } catch (Exception ee) {
                // do nothing
            }
        }

        /////////////////////////////////////////////////
        // Close Connections
        /////////////////////////////////////////////////
        try { repoConn.close(); } catch (Exception e) {
            // do nothing
        }

    }

}
