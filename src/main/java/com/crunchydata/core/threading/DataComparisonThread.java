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

package com.crunchydata.core.threading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.concurrent.BlockingQueue;

import com.crunchydata.controller.RepoController;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.model.DataComparisonResult;
import com.crunchydata.util.*;

import static com.crunchydata.service.DatabaseConnectionService.getConnection;
import static com.crunchydata.util.HashingUtils.getMd5;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_STAGETABLE_INSERT;
import static com.crunchydata.config.Settings.Props;

/**
 * Thread to pull data from source or target and load into the repository database.
 *
 * @author Brian Pace
 */
public class DataComparisonThread extends Thread {
    private final Integer tid, batchNbr, cid, nbrColumns, parallelDegree, threadNumber;
    private final String modColumn, pkList, stagingTable, targetType;
    private String sql;
    private final BlockingQueue<DataComparisonResult[]> q;
    private final ThreadSync ts;
    private final Boolean useDatabaseHash;
    
    // Constants for better maintainability
    private static final int QUEUE_WAIT_THRESHOLD = 100;
    private static final int QUEUE_WAIT_TARGET = 50;
    private static final int QUEUE_WAIT_SLEEP_MS = 1000;
    private static final int OBSERVER_SLEEP_MS = 1000;
    private static final int PROGRESS_REPORT_INTERVAL = 10000;
    private static final String SOURCE_TYPE = "source";

    public DataComparisonThread(Integer threadNumber, DataComparisonTable dct, DataComparisonTableMap dctm, ColumnMetadata cm, Integer cid, ThreadSync ts, Boolean useDatabaseHash, String stagingTable, BlockingQueue<DataComparisonResult[]> q) {
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
    }

    public void run() {
        String threadName = String.format("compare-%s-%s-t%s", targetType, tid, threadNumber);
        LoggingUtils.write("info", threadName, String.format("(%s) Start database reconcile thread", targetType));

        // Configuration variables
        int totalRows = 0;
        int batchCommitSize = Integer.parseInt(Props.getProperty("batch-commit-size"));
        int fetchSize = Integer.parseInt(Props.getProperty("batch-fetch-size"));
        boolean useLoaderThreads = Integer.parseInt(Props.getProperty("loader-threads")) > 0;
        boolean observerThrottle = Boolean.parseBoolean(Props.getProperty("observer-throttle"));
        int cntRecord = 0;
        boolean firstPass = true;
        DecimalFormat formatter = new DecimalFormat("#,###");
        int loadRowCount = Integer.parseInt(Props.getProperty("batch-progress-report-size"));
        int observerRowCount = Integer.parseInt(Props.getProperty("observer-throttle-size"));
        
        // Database resources
        Connection conn = null;
        Connection connRepo = null;
        ResultSet rs = null;
        PreparedStatement stmt = null;
        PreparedStatement stmtLoad = null;
        
        RepoController rpc = new RepoController();

        try {
            // Connect to Repository
            connRepo = initializeRepositoryConnection(threadName);
            
            // Connect to Source/Target
            conn = initializeSourceTargetConnection(threadName);

            // Load Reconcile Data
            if ( parallelDegree > 1 && !modColumn.isEmpty()) {
                if ("mssql".equals(Props.getProperty(targetType + "-type"))) {
                    sql += " AND " + modColumn + "%" + parallelDegree +" = "+threadNumber;
                } else {
                    sql += " AND mod(" + modColumn + "," + parallelDegree +")="+threadNumber;
                }
            }

            if (!pkList.isEmpty() && Props.getProperty("database-sort").equals("true")) {
                sql += " ORDER BY " + pkList;
            }

            //conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(fetchSize);
            rs = stmt.executeQuery();

            StringBuilder columnValue = new StringBuilder();

            if (!useLoaderThreads) {
                String sqlLoad = String.format(SQL_REPO_STAGETABLE_INSERT, stagingTable);
                connRepo.setAutoCommit(false);
                stmtLoad = connRepo.prepareStatement(sqlLoad);
            }

            DataComparisonResult[] dc = new DataComparisonResult[batchCommitSize];

            while (rs.next()) {
                columnValue.setLength(0);

                if (! useDatabaseHash) {
                    for (int i = 3; i < nbrColumns + 3; i++) {
                        columnValue.append(rs.getString(i));
                    }
                } else {
                    columnValue.append(rs.getString(3));
                }

                String pkHash = useDatabaseHash ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH"));
                String columnHash = useDatabaseHash ? columnValue.toString() : getMd5(columnValue.toString());

                if (useLoaderThreads) {
                    dc[cntRecord] = new DataComparisonResult(tid,null, pkHash, columnHash, rs.getString("PK").replace(",}","}"),null,threadNumber,batchNbr);
                } else {
                    stmtLoad.setInt(1, tid);
                    stmtLoad.setString(2, pkHash);
                    stmtLoad.setString(3, columnHash);
                    stmtLoad.setString(4, rs.getString("PK").replace(",}","}"));
                    stmtLoad.addBatch();
                }

                cntRecord++;
                totalRows++;

                if (totalRows % batchCommitSize == 0) {
                    if (useLoaderThreads) {
                        handleLoaderThreadBatch(threadName, dc, batchCommitSize);
                    } else {
                        handleDirectDatabaseBatch(stmtLoad, connRepo);
                    }
                    cntRecord = 0;
                }

                // Handle progress reporting
                if (totalRows % ((firstPass) ? PROGRESS_REPORT_INTERVAL : loadRowCount) == 0) {
                    LoggingUtils.write("info", threadName, String.format("(%s) Loaded %s rows", targetType, formatter.format(totalRows)));
                }

                // Handle observer coordination
                if (totalRows % ((firstPass) ? PROGRESS_REPORT_INTERVAL : observerRowCount) == 0) {
                    handleObserverCoordination(threadName, firstPass, observerThrottle, rpc, connRepo, cntRecord);
                    if (firstPass) {
                        firstPass = false;
                    }
                }

            }

            // Process remaining records
            if (cntRecord > 0) {
                processRemainingRecords(useLoaderThreads, dc, stmtLoad, rpc, connRepo, cntRecord);
            }

            LoggingUtils.write("info", threadName, String.format("(%s) Complete. Total rows loaded: %s", targetType, formatter.format(totalRows)));

            // Wait for queues to empty if using loader threads
            if (useLoaderThreads) {
                waitForQueuesToEmpty(threadName);
            }

        } catch (SQLException e) {
            LoggingUtils.write("severe", threadName, String.format("(%s) Database error: %s", targetType, e.getMessage()));
        } catch (Exception e) {
            LoggingUtils.write("severe", threadName, String.format("(%s) Error in reconciliation thread: %s", targetType, e.getMessage()));
        } finally {
            // Signal completion
            signalThreadCompletion();
            
            // Clean up resources
            cleanupResources(threadName, rs, stmt, stmtLoad, connRepo, conn);
        }
    }
    
    /**
     * Initializes repository connection with proper error handling.
     */
    private Connection initializeRepositoryConnection(String threadName) throws SQLException {
        LoggingUtils.write("info", threadName, String.format("(%s) Connecting to repository database", targetType));
        Connection connRepo = getConnection("postgres", "repo");
        
        if (connRepo == null) {
            throw new SQLException("Cannot connect to repository database");
        }
        connRepo.setAutoCommit(false);
        return connRepo;
    }
    
    /**
     * Initializes source/target connection with proper error handling.
     */
    private Connection initializeSourceTargetConnection(String threadName) throws SQLException {
        LoggingUtils.write("info", threadName, String.format("(%s) Connecting to database", targetType));
        Connection conn = getConnection(Props.getProperty(targetType + "-type"), targetType);
        
        if (conn == null) {
            throw new SQLException("Cannot connect to " + targetType + " database");
        }
        return conn;
    }
    
    /**
     * Handles batch processing for loader threads.
     */
    private void handleLoaderThreadBatch(String threadName, DataComparisonResult[] dc, int batchCommitSize) throws InterruptedException {
        if (q != null && q.size() == QUEUE_WAIT_THRESHOLD) {
            LoggingUtils.write("info", threadName, String.format("(%s) Waiting for Queue space", targetType));
            while (q.size() > QUEUE_WAIT_TARGET) {
                Thread.sleep(QUEUE_WAIT_SLEEP_MS);
            }
        }
        if (q != null) {
            q.put(dc);
        }
        dc = null;
        dc = new DataComparisonResult[batchCommitSize];
    }
    
    /**
     * Handles batch processing for direct database insertion.
     */
    private void handleDirectDatabaseBatch(PreparedStatement stmtLoad, Connection connRepo) throws SQLException {
        if (stmtLoad != null) {
            stmtLoad.executeLargeBatch();
            stmtLoad.clearBatch();
            connRepo.commit();
        }
    }
    
    /**
     * Handles observer coordination logic.
     */
    private void handleObserverCoordination(String threadName, boolean firstPass, boolean observerThrottle, 
                                         RepoController rpc, Connection connRepo, int cntRecord) throws Exception {
        if (firstPass || observerThrottle) {
            LoggingUtils.write("info", threadName, String.format("(%s) Wait for Observer", targetType));
            
            rpc.dcrUpdateRowCount(connRepo, targetType, cid, cntRecord);
            connRepo.commit();
            
            // Set waiting flags
            if (SOURCE_TYPE.equals(targetType)) {
                ts.sourceWaiting = true;
            } else {
                ts.targetWaiting = true;
            }
            
            ts.observerWait();
            
            // Clear waiting flags
            if (SOURCE_TYPE.equals(targetType)) {
                ts.sourceWaiting = false;
            } else {
                ts.targetWaiting = false;
            }
            
            LoggingUtils.write("info", threadName, String.format("(%s) Cleared by Observer", targetType));
        } else {
            LoggingUtils.write("info", threadName, String.format("(%s) Pause for Observer", targetType));
            Thread.sleep(OBSERVER_SLEEP_MS);
        }
    }
    
    /**
     * Processes remaining records after main loop.
     */
    private void processRemainingRecords(boolean useLoaderThreads, DataComparisonResult[] dc, PreparedStatement stmtLoad,
                                         RepoController rpc, Connection connRepo, int cntRecord) throws Exception {
        if (useLoaderThreads) {
            if (q != null) {
                q.put(dc);
            }
        } else {
            if (stmtLoad != null) {
                stmtLoad.executeBatch();
            }
        }
        rpc.dcrUpdateRowCount(connRepo, targetType, cid, cntRecord);
    }
    
    /**
     * Waits for queues to empty if using loader threads.
     */
    private void waitForQueuesToEmpty(String threadName) throws InterruptedException {
        if (q != null) {
            while (!q.isEmpty()) {
                LoggingUtils.write("info", threadName, String.format("(%s) Waiting for message queue to empty", targetType));
                Thread.sleep(QUEUE_WAIT_SLEEP_MS);
            }
            Thread.sleep(QUEUE_WAIT_SLEEP_MS);
        }
    }
    
    /**
     * Signals thread completion.
     */
    private void signalThreadCompletion() {
        if (SOURCE_TYPE.equals(targetType)) {
            ts.sourceComplete = true;
        } else {
            ts.targetComplete = true;
        }
    }
    
    /**
     * Cleans up database resources.
     */
    private void cleanupResources(String threadName, ResultSet rs, PreparedStatement stmt, 
                                PreparedStatement stmtLoad, Connection connRepo, Connection conn) {
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
            if (connRepo != null) {
                connRepo.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            LoggingUtils.write("warning", threadName, String.format("(%s) Error closing connections: %s", targetType, e.getMessage()));
        }

    }


}
