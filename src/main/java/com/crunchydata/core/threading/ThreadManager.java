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

import com.crunchydata.controller.RepoController;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.model.DataComparisonResult;
import com.crunchydata.util.LoggingUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.crunchydata.config.Settings.Props;

/**
 * Manager class for coordinating thread operations during data reconciliation.
 * This class handles the complex thread coordination required for parallel
 * data processing, including compare threads, loader threads, and observer threads.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class ThreadManager {
    
    private static final String THREAD_NAME = "thread-manager";
    private static final int THREAD_SLEEP_MS = 2000;
    
    // Thread collections
    private static final List<DataComparisonThread> compareList = new ArrayList<>();
    private static final List<DataLoaderThread> loaderList = new ArrayList<>();
    private static final List<ReconciliationObserverThread> observerList = new ArrayList<>();
    
    /**
     * Execute reconciliation using coordinated thread management.
     * 
     * @param dct Table information
     * @param cid Compare ID
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     * @param connRepo Repository connection
     * @throws InterruptedException if thread operations are interrupted
     */
    public static void executeReconciliation(DataComparisonTable dct, Integer cid, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                             ColumnMetadata ciSource, ColumnMetadata ciTarget, Connection connRepo)
                                           throws InterruptedException {
        
        // Clear previous thread lists
        clearThreadLists();
        
        // Configure thread settings
        boolean useLoaderThreads = (Integer.parseInt(Props.getProperty("loader-threads")) > 0);
        int messageQueueSize = Integer.parseInt(Props.getProperty("message-queue-size"));
        
        // Create blocking queues for thread communication
        BlockingQueue<DataComparisonResult[]> qs = useLoaderThreads ? new ArrayBlockingQueue<>(messageQueueSize) : null;
        BlockingQueue<DataComparisonResult[]> qt = useLoaderThreads ? new ArrayBlockingQueue<>(messageQueueSize) : null;
        
        LoggingUtils.write("info", THREAD_NAME, "Starting compare hash threads");
        
        // Start reconciliation threads
        startReconcileThreads(dct, cid, dctmSource, dctmTarget, ciSource, ciTarget, qs, qt, useLoaderThreads, connRepo);
        
        // Wait for completion
        waitForThreadCompletion();
    }
    
    /**
     * Clear all thread lists.
     */
    private static void clearThreadLists() {
        compareList.clear();
        loaderList.clear();
        observerList.clear();
    }
    
    /**
     * Start reconciliation threads for parallel processing.
     * 
     * @param dct Table information
     * @param cid Compare ID
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     * @param qs Source queue
     * @param qt Target queue
     * @param useLoaderThreads Whether to use loader threads
     * @param connRepo Repository connection
     * @throws InterruptedException if thread operations are interrupted
     */
    private static void startReconcileThreads(DataComparisonTable dct, Integer cid, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                              ColumnMetadata ciSource, ColumnMetadata ciTarget,
                                              BlockingQueue<DataComparisonResult[]> qs, BlockingQueue<DataComparisonResult[]> qt,
                                              boolean useLoaderThreads, Connection connRepo)
                                             throws InterruptedException {
        
        RepoController rpc = new RepoController();
        String columnHashMethod = Props.getProperty("column-hash-method");
        boolean useDatabaseHash = columnHashMethod.equals("database");
        
        for (int i = 0; i < dct.getParallelDegree(); i++) {
            // Create thread synchronization object
            ThreadSync ts = new ThreadSync();
            
            // Create staging tables
            String stagingSource = rpc.createStagingTable(connRepo, "source", dct.getTid(), i);
            String stagingTarget = rpc.createStagingTable(connRepo, "target", dct.getTid(), i);
            
            // Create and start observer thread
            ReconciliationObserverThread observer = new ReconciliationObserverThread(dct, cid, ts, i, stagingSource, stagingTarget);
            observer.start();
            observerList.add(observer);
            
            // Create and start compare threads
            DataComparisonThread srcThread = new DataComparisonThread(i, dct, dctmSource, ciSource, cid, ts, useDatabaseHash, stagingSource, qs);
            DataComparisonThread tgtThread = new DataComparisonThread(i, dct, dctmTarget, ciTarget, cid, ts, useDatabaseHash, stagingTarget, qt);
            
            srcThread.start();
            compareList.add(srcThread);
            
            tgtThread.start();
            compareList.add(tgtThread);
            
            // Create and start loader threads if enabled
            if (useLoaderThreads) {
                startLoaderThreads(i, qs, qt, stagingSource, stagingTarget, ts);
            }
            
            // Brief pause between thread creation
            Thread.sleep(THREAD_SLEEP_MS);
        }
    }
    
    /**
     * Start loader threads for data processing.
     * 
     * @param threadIndex Thread index
     * @param qs Source queue
     * @param qt Target queue
     * @param stagingSource Source staging table
     * @param stagingTarget Target staging table
     * @param ts Thread synchronization object
     */
    private static void startLoaderThreads(int threadIndex, BlockingQueue<DataComparisonResult[]> qs, BlockingQueue<DataComparisonResult[]> qt,
                                           String stagingSource, String stagingTarget, ThreadSync ts) {
        int loaderThreads = Integer.parseInt(Props.getProperty("loader-threads"));
        
        for (int li = 1; li <= loaderThreads; li++) {
            DataLoaderThread loaderSrc = new DataLoaderThread(threadIndex, li, "source", qs, stagingSource, ts);
            DataLoaderThread loaderTgt = new DataLoaderThread(threadIndex, li, "target", qt, stagingTarget, ts);
            
            loaderSrc.start();
            loaderList.add(loaderSrc);
            
            loaderTgt.start();
            loaderList.add(loaderTgt);
        }
    }
    
    /**
     * Wait for all threads to complete.
     * 
     * @throws InterruptedException if thread operations are interrupted
     */
    private static void waitForThreadCompletion() throws InterruptedException {
        LoggingUtils.write("info", THREAD_NAME, "Waiting for compare threads to complete");
        joinThreads(compareList);
        
        LoggingUtils.write("info", THREAD_NAME, "Waiting for reconcile threads to complete");
        joinThreads(observerList);
        
        LoggingUtils.write("info", THREAD_NAME, "All reconciliation threads completed");
    }
    
    /**
     * Join all threads in the provided list.
     * 
     * @param threads List of threads to join
     * @throws InterruptedException if thread operations are interrupted
     */
    private static void joinThreads(List<? extends Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                thread.join();
            }
        }
    }
    
    /**
     * Get the number of active compare threads.
     * 
     * @return Number of active compare threads
     */
    public static int getActiveCompareThreads() {
        return (int) compareList.stream().filter(Thread::isAlive).count();
    }
    
    /**
     * Get the number of active loader threads.
     * 
     * @return Number of active loader threads
     */
    public static int getActiveLoaderThreads() {
        return (int) loaderList.stream().filter(Thread::isAlive).count();
    }
    
    /**
     * Get the number of active observer threads.
     * 
     * @return Number of active observer threads
     */
    public static int getActiveObserverThreads() {
        return (int) observerList.stream().filter(Thread::isAlive).count();
    }
    
    /**
     * Get total number of active threads.
     * 
     * @return Total number of active threads
     */
    public static int getTotalActiveThreads() {
        return getActiveCompareThreads() + getActiveLoaderThreads() + getActiveObserverThreads();
    }
    
    /**
     * Check if all threads have completed.
     * 
     * @return true if all threads are completed, false otherwise
     */
    public static boolean areAllThreadsCompleted() {
        return getTotalActiveThreads() == 0;
    }
    
    /**
     * Interrupt all active threads.
     */
    public static void interruptAllThreads() {
        LoggingUtils.write("warning", THREAD_NAME, "Interrupting all active threads");
        
        compareList.forEach(Thread::interrupt);
        loaderList.forEach(Thread::interrupt);
        observerList.forEach(Thread::interrupt);
    }
    
    /**
     * Get thread status information.
     * 
     * @return Thread status information
     */
    public static String getThreadStatus() {
        return String.format("Threads - Compare: %d, Loader: %d, Observer: %d, Total: %d",
            getActiveCompareThreads(), getActiveLoaderThreads(), getActiveObserverThreads(), getTotalActiveThreads());
    }
}
