/*
 * Copyright 2012-2025 the original author or authors.
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

import com.crunchydata.model.DataComparisonResult;
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.crunchydata.service.DatabaseConnectionService.getConnection;


/**
 * Thread class responsible for loading data into the repository database.
 *
 * <p>This class extends Thread and implements the logic to retrieve DataCompare objects
 * from a blocking queue and insert them into a staging table in the repository database.</p>
 *
 * <p>The loader thread runs until both source and target complete flags are set to true
 * in the ThreadSync object provided during initialization.</p>
 *
 * @author Brian Pace
 */
public class DataLoaderThread extends Thread  {
    private final BlockingQueue<DataComparisonResult[]> q;
    private final Integer instanceNumber;
    private final String stagingTable;
    private final String targetType;
    private final Integer threadNumber;
    private final ThreadSync ts;
    
    // Constants for better maintainability
    private static final int DEFAULT_QUEUE_POLL_TIMEOUT_MS = 500;
    private static final String STAGING_INSERT_SQL = "INSERT INTO %s (tid, pk_hash, column_hash, pk) VALUES (?, ?,?,(?)::jsonb)";
    private static final String POSTGRES_OPTIMIZATION_SYNC_COMMIT = "set synchronous_commit='off'";
    private static final String POSTGRES_OPTIMIZATION_WORK_MEM = "set work_mem='256MB'";

    /**
     * Constructor for initializing a dbLoader instance.
     *
     * @param threadNumber The number identifying the thread within its type.
     * @param instanceNumber The instance number of the thread.
     * @param targetType The type of data being loaded ("source" or "target").
     * @param q The BlockingQueue containing DataCompare objects to load.
     * @param stagingTable The name of the staging table in the repository database.
     * @param ts The ThreadSync object for coordinating thread synchronization.
     */
    public DataLoaderThread(Integer threadNumber, Integer instanceNumber, String targetType, BlockingQueue<DataComparisonResult[]> q, String stagingTable, ThreadSync ts) {
        this.q = q;
        this.instanceNumber = instanceNumber;
        this.stagingTable = stagingTable;
        this.targetType = targetType;
        this.threadNumber = threadNumber;
        this.ts = ts;
    }

    /**
     * Runs the loader thread logic.
     *
     * <p>The thread connects to the repository database, prepares an INSERT statement
     * for the staging table, and continuously polls the blocking queue for DataCompare
     * objects to insert. It commits batches of inserts and checks ThreadSync flags to
     * determine when to stop loading.</p>
     */
    @Override
    public void run() {
        String threadName = String.format("loader-%s-t%s-i%s", targetType, threadNumber, instanceNumber);
        LoggingUtils.write("info", threadName, "Start repository loader thread");

        Connection connRepo = null;
        PreparedStatement stmtLoad = null;

        try {
            // Initialize repository connection
            connRepo = initializeRepositoryConnection(threadName);
            
            // Prepare INSERT statement for the staging table
            stmtLoad = prepareStagingInsertStatement(connRepo);

            // Main data loading loop
            executeDataLoading(threadName, stmtLoad, connRepo);

            LoggingUtils.write("info", threadName, "Loader thread complete.");

        } catch (SQLException e) {
            LoggingUtils.write("severe", threadName, String.format("Database error: %s", e.getMessage()));
        } catch (Exception e) {
            LoggingUtils.write("severe", threadName, String.format("Error in loader thread: %s", e.getMessage()));
        } finally {
            // Clean up resources and signal completion
            cleanupResources(threadName, stmtLoad, connRepo);
            signalThreadCompletion();
        }
    }
    
    /**
     * Initializes repository connection with proper error handling.
     */
    private Connection initializeRepositoryConnection(String threadName) throws SQLException {
        LoggingUtils.write("info", threadName, "Connecting to repository database");
        Connection connRepo = getConnection("postgres", "repo");

        if (connRepo == null) {
            throw new SQLException("Cannot connect to repository database");
        }

        // Apply Postgres optimizations
        SQLExecutionHelper.simpleExecute(connRepo, POSTGRES_OPTIMIZATION_SYNC_COMMIT);
        SQLExecutionHelper.simpleExecute(connRepo, POSTGRES_OPTIMIZATION_WORK_MEM);
        connRepo.setAutoCommit(false);
        
        return connRepo;
    }
    
    /**
     * Prepares the staging table INSERT statement.
     */
    private PreparedStatement prepareStagingInsertStatement(Connection connRepo) throws SQLException {
        String sqlLoad = String.format(STAGING_INSERT_SQL, stagingTable);
        return connRepo.prepareStatement(sqlLoad);
    }
    
    /**
     * Executes the main data loading logic.
     */
    private void executeDataLoading(String threadName, PreparedStatement stmtLoad, Connection connRepo) throws Exception {
        boolean stillLoading = true;

        // Main loop to load data into the repository
        while (stillLoading) {
            // Poll for DataCompare array from the blocking queue
            DataComparisonResult[] dc = q.poll(DEFAULT_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (dc != null && dc.length > 0) {
                processDataCompareArray(dc, stmtLoad, connRepo);
            }

            // Check if both source and target are complete
            if (ts.sourceComplete && ts.targetComplete) {
                stillLoading = false;
            }
        }
    }
    
    /**
     * Processes a DataCompare array and inserts into database.
     */
    private void processDataCompareArray(DataComparisonResult[] dc, PreparedStatement stmtLoad, Connection connRepo) throws SQLException {
        for (DataComparisonResult dataComparisonResult : dc) {
            if (dataComparisonResult != null && dataComparisonResult.getPk() != null) {
                stmtLoad.setInt(1, dataComparisonResult.getTid());
                stmtLoad.setString(2, dataComparisonResult.getPkHash());
                stmtLoad.setString(3, dataComparisonResult.getColumnHash());
                stmtLoad.setString(4, dataComparisonResult.getPk());
                stmtLoad.addBatch();
                stmtLoad.clearParameters();
            } else {
                // Exit loop if null or incomplete DataCompare object
                break;
            }
        }

        // Execute batch insert and commit transaction
        stmtLoad.executeBatch();
        stmtLoad.clearBatch();
        connRepo.commit();
    }
    
    /**
     * Cleans up database resources.
     */
    private void cleanupResources(String threadName, PreparedStatement stmtLoad, Connection connRepo) {
        try {
            if (stmtLoad != null) {
                stmtLoad.close();
            }
            if (connRepo != null) {
                connRepo.close();
            }
        } catch (Exception e) {
            LoggingUtils.write("warning", threadName, String.format("Error closing connections: %s", e.getMessage()));
        }
    }
    
    /**
     * Signals thread completion.
     */
    private void signalThreadCompletion() {
        ts.incrementLoaderThreadComplete();
    }
}
