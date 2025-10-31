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

package com.crunchydata.core.comparison;

import com.crunchydata.service.SQLExecutionService;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Processor class for handling reconciliation result summarization and processing.
 * This class encapsulates the logic for analyzing reconciliation results,
 * calculating statistics, and updating the database with final results.
 * 
 * @author Brian Pace
 */
public class ResultProcessor {
    
    private static final String THREAD_NAME = "result-processor";
    
    /**
     * Summarize reconciliation results and update the database.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param result Result object to update
     * @param cid Compare ID
     * @throws SQLException if database operations fail
     */
    public static void summarizeResults(Connection connRepo, long tid, JSONObject result, int cid) throws SQLException {
        LoggingUtils.write("info", THREAD_NAME, "Starting result summarization");
        
        try {
            // Optimize database for result processing
            optimizeDatabaseForResults(connRepo);
            
            // Calculate reconciliation statistics
            ReconciliationStats stats = calculateReconciliationStats(connRepo, tid);
            
            // Update result object with statistics
            updateResultWithStats(result, stats);
            
            // Update database with final results
            updateDatabaseResults(connRepo, result, cid);
            
            LoggingUtils.write("info", THREAD_NAME, "Result summarization completed successfully");
            
        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error during result summarization: %s", e.getMessage()));
            throw e;
        }
    }
    
    /**
     * Optimize database settings for result processing.
     * 
     * @param connRepo Repository connection
     * @throws SQLException if database operations fail
     */
    private static void optimizeDatabaseForResults(Connection connRepo) throws SQLException {
        SQLExecutionService.simpleExecute(connRepo, "set enable_nestloop='off'");
    }
    
    /**
     * Calculate reconciliation statistics.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @return Reconciliation statistics
     * @throws SQLException if database operations fail
     */
    private static ReconciliationStats calculateReconciliationStats(Connection connRepo, long tid) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(tid);
        
        // Calculate missing source records
        int missingSource = SQLExecutionService.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKMISSING, binds, true);
        
        // Calculate missing target records
        int missingTarget = SQLExecutionService.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKMISSING, binds, true);
        
        // Calculate not equal records
        int notEqual = SQLExecutionService.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_MARKNOTEQUAL, binds, true);
        SQLExecutionService.simpleUpdate(connRepo, SQL_REPO_DCTARGET_MARKNOTEQUAL, binds, true);
        
        return new ReconciliationStats(missingSource, missingTarget, notEqual);
    }
    
    /**
     * Update result object with calculated statistics.
     * 
     * @param result Result object to update
     * @param stats Reconciliation statistics
     */
    private static void updateResultWithStats(JSONObject result, ReconciliationStats stats) {
        result.put("missingSource", stats.missingSource());
        result.put("missingTarget", stats.missingTarget());
        result.put("notEqual", stats.notEqual());
        
        // Determine final status
        if ("processing".equals(result.getString("compareStatus"))) {
            boolean hasOutOfSyncRecords = stats.missingSource() + stats.missingTarget() + stats.notEqual() > 0;
            result.put("compareStatus", hasOutOfSyncRecords ? "out-of-sync" : "in-sync");
        }
    }
    
    /**
     * Update database with final reconciliation results.
     * 
     * @param connRepo Repository connection
     * @param result Result object
     * @param cid Compare ID
     * @throws SQLException if database operations fail
     */
    private static void updateDatabaseResults(Connection connRepo, JSONObject result, int cid) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(result.getInt("missingSource"));
        binds.add(result.getInt("missingTarget"));
        binds.add(result.getInt("notEqual"));
        binds.add(result.getString("compareStatus"));
        binds.add(cid);
        
        try (var crs = SQLExecutionService.simpleUpdateReturning(connRepo, SQL_REPO_DCRESULT_UPDATE_STATUSANDCOUNT, binds)) {
            if (crs.next()) {
                int equal = crs.getInt(1);
                result.put("equal", equal);
                result.put("totalRows", equal + result.getInt("missingSource") + result.getInt("missingTarget") + result.getInt("notEqual"));
            }
        }
    }


    /**
         * Inner class to hold reconciliation statistics.
         */
        public record ReconciliationStats(int missingSource, int missingTarget, int notEqual) {
    }
}
