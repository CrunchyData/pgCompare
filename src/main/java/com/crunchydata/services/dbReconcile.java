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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.crunchydata.controller.RepoController;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.*;

import javax.xml.crypto.Data;

import static com.crunchydata.util.SecurityUtility.getMd5;
import static com.crunchydata.util.Settings.Props;

public class dbReconcile extends Thread {
    Integer batchNbr;
    Integer cid;
    String modColumn;
    Integer nbrColumns;
    Integer nbrPKColumns;
    Integer parallelDegree;
    String pkList;
    BlockingQueue<DataCompare[]> q;
    String schemaName;
    String sql;
    String stagingTable;
    String tableName;
    String tableFilter;
    String targetType;
    String threadName;
    Integer threadNumber;
    Integer tid;
    ThreadSync ts;
    Boolean useDatabaseHash;
    Boolean useLoaderThreads;

    public dbReconcile(Integer threadNumber, String targetType, String sql, String tableFilter, String modColumn, Integer parallelDegree, String schemaName, String tableName, Integer nbrColumns, Integer nbrPKColumns, Integer cid, ThreadSync ts, String pkList, Boolean useDatabaseHash, Integer batchNbr, Integer tid, String stagingTable, BlockingQueue<DataCompare[]> q) {
        this.q = q;
        this.modColumn = modColumn;
        this.parallelDegree = parallelDegree;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sql = sql;
        this.tableFilter = tableFilter;
        this.targetType = targetType;
        this.threadNumber = threadNumber;
        this.nbrColumns = nbrColumns;
        this.nbrPKColumns = nbrPKColumns;
        this.cid = cid;
        this.ts = ts;
        this.pkList = pkList;
        this.useDatabaseHash = useDatabaseHash;
        this.batchNbr = batchNbr;
        this.tid = tid;
        this.stagingTable = stagingTable;
    }

    public void run() {

        threadName = "reconcile-"+targetType+"-"+threadNumber;
        Logging.write("info", threadName, "Start database reconcile thread");

        useLoaderThreads = Integer.parseInt(Props.getProperty("message-queue-size")) == 0;

        int cntRecord = 0;
        Connection conn = null;
        boolean firstPass = true;
        int loadRowCount = 10000;
        int observerRowCount = 10000;
        Connection repoConn = null;
        RepoController rpc = new RepoController();
        ResultSet rs = null;
        PreparedStatement stmt = null;
        PreparedStatement stmtLoad = null;
        int totalRows = 0;

        try {
            /////////////////////////////////////////////////
            // Connect to Repository
            /////////////////////////////////////////////////
            Logging.write("info", threadName, "Connecting to repository database");
            repoConn = dbPostgres.getConnection(Props,"repo", "reconcile");
            if ( repoConn == null) {
                Logging.write("severe", threadName, "Cannot connect to repository database");
                System.exit(1);
            }
            repoConn.setAutoCommit(false);

            /////////////////////////////////////////////////
            // Connect to Source/Target
            /////////////////////////////////////////////////
            Logging.write("info", threadName, "Connecting to " + targetType + " database");

            switch (Props.getProperty(targetType + "-type")) {
                case "oracle":
                    conn = dbOracle.getConnection(Props,targetType);
                    break;
                case "mysql":
                    conn = dbMySQL.getConnection(Props,targetType);
                    break;
                case "mssql":
                    conn = dbMSSQL.getConnection(Props,targetType);
                    break;
                default:
                    conn = dbPostgres.getConnection(Props,targetType, "reconcile");
                    conn.setAutoCommit(false);
                    break;
            }

            if ( conn == null) {
                Logging.write("severe", threadName, "Cannot connect to " + targetType + " database");
                System.exit(1);
            }

            /////////////////////////////////////////////////
            // Load Reconcile Data
            /////////////////////////////////////////////////
            if ( parallelDegree > 1 && !modColumn.isEmpty()) {
                sql += " AND mod(" + modColumn + "," + parallelDegree +")="+threadNumber;
            }

            if (!pkList.isEmpty() && Props.getProperty("database-sort").equals("true")) {
                sql += " ORDER BY " + pkList;
            }

            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(Integer.parseInt(Props.getProperty("batch-fetch-size")));
            rs = stmt.executeQuery();

            StringBuilder columnValue = new StringBuilder();

            if (! useLoaderThreads) {
                String sqlLoad = "INSERT INTO " + stagingTable + " (pk_hash, column_hash, pk) VALUES (?,?,(?)::jsonb)";
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

                if ( useLoaderThreads ) {
                    dc[cntRecord] = new DataCompare(null,(useDatabaseHash) ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")),(useDatabaseHash) ? columnValue.toString() : getMd5(columnValue.toString()), rs.getString("PK").replace(",}","}"),null,threadNumber,batchNbr);
                } else {
                    stmtLoad.setString(1, (useDatabaseHash) ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")));
                    stmtLoad.setString(2, (useDatabaseHash) ? columnValue.toString() : getMd5(columnValue.toString()));
                    stmtLoad.setString(3, rs.getString("PK").replace(",}","}"));
                    stmtLoad.addBatch();
                    stmtLoad.clearParameters();
                }

                cntRecord++;
                totalRows++;

                if (totalRows % Integer.parseInt(Props.getProperty("batch-commit-size")) == 0 ) {
                    if ( useLoaderThreads) {
                        if ( q.size() == 100) {
                            Logging.write("info", threadName, "Waiting for Queue space");
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
                    Logging.write("info", threadName, "Loaded " + totalRows + " rows");
                }

                if (totalRows % observerRowCount == 0) {
                    if (firstPass || Boolean.parseBoolean(Props.getProperty("observer-throttle"))) {
                        firstPass = false;

                        Logging.write("info", threadName, "Wait for Observer");

                        rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);

                        repoConn.commit();

                        cntRecord=0;

                        if ( targetType.equals("source")) {
                            ts.sourceWaiting = true;
                        } else {
                            ts.targetWaiting = true;
                        }

                        ts.ObserverWait();

                        if ( targetType.equals("source")) {
                            ts.sourceWaiting = false;
                        } else {
                            ts.targetWaiting = false;
                        }

                        Logging.write("info", threadName, "Cleared by Observer");
                    } else {
                        Logging.write("info", threadName, "Pause for Observer");
                        Thread.sleep(1000);
                    }
                }

                if (!firstPass) {
                    loadRowCount = Integer.parseInt(Props.getProperty("batch-progress-report-size"));
                    observerRowCount = Integer.parseInt(Props.getProperty("observer-throttle-size"));
                }

            }

            if ( cntRecord > 0 ) {
                if ( useLoaderThreads ) {
                    q.put(dc);
                } else {
                    stmtLoad.executeBatch();
                }
                rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
            }

            Logging.write("info", threadName, "Complete. Total rows loaded: " + totalRows);

            /////////////////////////////////////////////////
            // Wait for Queues to Empty
            /////////////////////////////////////////////////
            if ( useLoaderThreads) {
                while (q.size() > 0 ) {
                    Logging.write("info", threadName, "Waiting for message queue to empty");
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
            Logging.write("severe", threadName, "Database error " + e.getMessage());
        } catch (Exception e) {
            Logging.write("severe", threadName, "Error in reconciliation thread " + e.getMessage());
            e.printStackTrace();
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

                /////////////////////////////////////////////////
                // Close Connections
                /////////////////////////////////////////////////
                if (repoConn != null) {
                    repoConn.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
}
