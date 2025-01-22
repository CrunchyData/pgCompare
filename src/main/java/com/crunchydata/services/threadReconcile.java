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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import com.crunchydata.controller.RepoController;
import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.models.DataCompare;
import com.crunchydata.util.*;

import static com.crunchydata.util.HashUtility.getMd5;

/**
 * Thread to pull data from source or target and load into the repository database.
 *
 * @author Brian Pace
 */
public class threadReconcile extends Thread {
    private final Integer tid;
    private final Integer batchNbr;
    private final Integer cid;
    private final String modColumn;
    private final Integer nbrColumns;
    private final Integer parallelDegree;
    private final String pkList;
    private BlockingQueue<DataCompare[]> q;
    private String sql;
    private final String stagingTable;
    private final String targetType;
    private final Integer threadNumber;
    private final ThreadSync ts;
    private final Boolean useDatabaseHash;
    private Properties Props;

    public threadReconcile(Properties Props, Integer threadNumber, DCTable dct, DCTableMap dctm, ColumnMetadata cm, Integer cid, ThreadSync ts, Boolean useDatabaseHash, String stagingTable, BlockingQueue<DataCompare[]> q) {
        this.q = q;
        this.modColumn = dctm.getModColumn();
        this.parallelDegree = dct.getParallelDegree();
        this.sql = dctm.getCompareSQL();
        this.targetType = dctm.getDestType();
        this.threadNumber = threadNumber;
        this.nbrColumns = cm.getNbrColumns();
        this.tid = dct.getTid();
        this.cid = cid;
        this.ts = ts;
        this.pkList = cm.getPkList();
        this.useDatabaseHash = useDatabaseHash;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTable = stagingTable;
        this.Props = Props;
    }

    public void run() {

        String threadName = String.format("Reconcile-%s-c%s-t%s", targetType, cid, threadNumber);
        Logging.write("info", threadName, String.format("(%s) Start database reconcile thread",targetType));

        int cntRecord = 0;
        Connection conn = null;
        boolean firstPass = true;
        DecimalFormat formatter = new DecimalFormat("#,###");
        int loadRowCount = 10000;
        int observerRowCount = 10000;
        Connection repoConn = null;
        RepoController rpc = new RepoController();
        ResultSet rs = null;
        PreparedStatement stmt = null;
        PreparedStatement stmtLoad = null;
        int totalRows = 0;
        boolean useLoaderThreads = Integer.parseInt(Props.getProperty("loader-threads")) > 0;

        try {
            // Connect to Repository
            Logging.write("info", threadName, String.format("(%s) Connecting to repository database", targetType));
            repoConn = dbPostgres.getConnection(Props,"repo", "reconcile");

            if ( repoConn == null) {
                Logging.write("severe", threadName, String.format("(%s) Cannot connect to repository database", targetType));
                System.exit(1);
            }
            repoConn.setAutoCommit(false);

            // Connect to Source/Target
            Logging.write("info", threadName, String.format("(%s) Connecting to database", targetType));

            switch (Props.getProperty(targetType + "-type")) {
                case "oracle":
                    conn = dbOracle.getConnection(Props,targetType);
                    break;
                case "mariadb":
                    conn = dbMariaDB.getConnection(Props,targetType);
                    break;
                case "mysql":
                    conn = dbMySQL.getConnection(Props,targetType);
                    break;
                case "mssql":
                    conn = dbMSSQL.getConnection(Props,targetType);
                    break;
                case "db2":
                    conn = dbDB2.getConnection(Props,targetType);
                    break;
                default:
                    conn = dbPostgres.getConnection(Props,targetType, "reconcile");
                    conn.setAutoCommit(false);
                    break;
            }

            if ( conn == null) {
                Logging.write("severe", threadName, String.format("(%s) Cannot connect to database", targetType));
                System.exit(1);
            }

            // Load Reconcile Data
            if ( parallelDegree > 1 && !modColumn.isEmpty()) {
                sql += " AND mod(" + modColumn + "," + parallelDegree +")="+threadNumber;
            }

            if (!pkList.isEmpty() && Props.getProperty("database-sort").equals("true")) {
                sql += " ORDER BY " + pkList;
            }

            //conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(Integer.parseInt(Props.getProperty("batch-fetch-size")));
            rs = stmt.executeQuery();

            StringBuilder columnValue = new StringBuilder();

            if (!useLoaderThreads) {
                String sqlLoad = "INSERT INTO " + stagingTable + " (tid, pk_hash, column_hash, pk) VALUES (?,?,?,(?)::jsonb)";
                repoConn.setAutoCommit(false);
                stmtLoad = repoConn.prepareStatement(sqlLoad);
            }

            DataCompare[] dc = new DataCompare[Integer.parseInt(Props.getProperty("batch-commit-size"))];

            while (rs.next()) {
                columnValue.setLength(0);

                if (! useDatabaseHash) {
                    for (int i = 3; i < nbrColumns + 3; i++) {
                        columnValue.append(rs.getString(i));
                    }
                } else {
                    columnValue.append(rs.getString(3));
                }

                if (useLoaderThreads) {
                    dc[cntRecord] = new DataCompare(tid,null,(useDatabaseHash) ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")),(useDatabaseHash) ? columnValue.toString() : getMd5(columnValue.toString()), rs.getString("PK").replace(",}","}"),null,threadNumber,batchNbr);
                } else {
                    stmtLoad.setInt(1, tid);
                    stmtLoad.setString(2, (useDatabaseHash) ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")));
                    stmtLoad.setString(3, (useDatabaseHash) ? columnValue.toString() : getMd5(columnValue.toString()));
                    stmtLoad.setString(4, rs.getString("PK").replace(",}","}"));
                    stmtLoad.addBatch();
                    stmtLoad.clearParameters();
                }

                cntRecord++;
                totalRows++;

                if (totalRows % Integer.parseInt(Props.getProperty("batch-commit-size")) == 0 ) {
                    if (useLoaderThreads) {
                        if ( q.size() == 100) {
                            Logging.write("info", threadName, String.format("(%s) Waiting for Queue space", targetType));
                            while (q.size() > 50) {
                                Thread.sleep(1000);
                            }
                        }
                        q.put(dc);
                        dc = null;
                        dc = new DataCompare[Integer.parseInt(Props.getProperty("batch-commit-size"))];
                    } else {
                        stmtLoad.executeLargeBatch();
                        stmtLoad.clearBatch();
                        repoConn.commit();
                    }
                    cntRecord=0;
                }

                if (totalRows % loadRowCount == 0) {
                    Logging.write("info", threadName, String.format("(%s) Loaded %s rows", targetType, formatter.format(totalRows)));
                }

                if (totalRows % observerRowCount == 0) {
                    if (firstPass || Boolean.parseBoolean(Props.getProperty("observer-throttle"))) {
                        firstPass = false;

                        Logging.write("info", threadName, String.format("(%s) Wait for Observer", targetType));

                        rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);

                        repoConn.commit();

                        cntRecord=0;

                        if ( targetType.equals("source")) {
                            ts.sourceWaiting = true;
                        } else {
                            ts.targetWaiting = true;
                        }

                        ts.observerWait();

                        if ( targetType.equals("source")) {
                            ts.sourceWaiting = false;
                        } else {
                            ts.targetWaiting = false;
                        }

                        Logging.write("info", threadName, String.format("(%s) Cleared by Observer",targetType));
                    } else {
                        Logging.write("info", threadName, String.format("(%s) Pause for Observer",targetType));
                        Thread.sleep(1000);
                    }
                }

                if (!firstPass) {
                    loadRowCount = Integer.parseInt(Props.getProperty("batch-progress-report-size"));
                    observerRowCount = Integer.parseInt(Props.getProperty("observer-throttle-size"));
                }

            }

            if ( cntRecord > 0 ) {
                if (useLoaderThreads) {
                    q.put(dc);
                } else {
                    stmtLoad.executeBatch();
                }
                rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
            }

            Logging.write("info", threadName, String.format("(%s) Complete. Total rows loaded: %s", targetType, formatter.format(totalRows)));

            // Wait for Queues to Empty
            if (useLoaderThreads) {
                while (!q.isEmpty()) {
                    Logging.write("info", threadName, String.format("(%s) Waiting for message queue to empty",targetType));
                    Thread.sleep(1000);
                }
                Thread.sleep(1000);
            }

            if ( targetType.equals("source")) {
                ts.sourceComplete = true;
            } else {
                ts.targetComplete = true;
            }

        } catch( SQLException e) {
            Logging.write("severe", threadName, String.format("(%s) Database error:  %s", targetType, e.getMessage()));
        } catch (Exception e) {
            Logging.write("severe", threadName, String.format("(%s) Error in reconciliation thread:  %s", targetType, e.getMessage()));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }

                if (stmtLoad != null) {
                    stmtLoad.close();
                }

                // Close Connections
                if (repoConn != null) {
                    repoConn.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                Logging.write("severe", threadName, String.format("(%s) Error closing connections thread:  %s", targetType, e.getMessage()));
            }
        }


    }
}
