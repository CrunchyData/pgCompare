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

package com.crunchydata.controller;

import com.crunchydata.config.ApplicationContext;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.service.DataReconciliationService;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;

import static com.crunchydata.config.Settings.Props;

import org.json.JSONObject;

/**
 * CompareController class that provides a simplified interface for data reconciliation.
 * This class has been refactored to use specialized services for better separation
 * of concerns and improved maintainability.
 *
 * @author Brian Pace
 * @version 1.0
 */
public class CompareController {

    private static final String THREAD_NAME = "compare-ctrl";

    /**
     * Reconciles data between source and target databases using the optimized DataReconciliationService.
     *
     * @param connRepo       Connection to the repository database
     * @param connSource     Connection to the source database
     * @param connTarget     Connection to the target database
     * @param rid            Reconciliation ID
     * @param check          Whether to perform a check
     * @param dct            Table information
     * @param dctmSource     Source table map
     * @param dctmTarget     Target table map
     * @return JSON object with reconciliation results
     */
    public static JSONObject reconcileData(Connection connRepo, Connection connSource, Connection connTarget,
                                           long rid, Boolean check, DataComparisonTable dct, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget) {
        try {
            // Validate inputs
            validateReconcileInputs(connRepo, connSource, connTarget, dct, dctmSource, dctmTarget);
            
            // Use the optimized reconciliation service
            return DataReconciliationService.reconcileData(connRepo, connSource, connTarget, rid, check, dct, dctmSource, dctmTarget);
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error during data reconciliation: %s", e.getMessage()));
            return createErrorResult(dct.getTableAlias(), e.getMessage());
        }
    }
    
    /**
     * Validate inputs for reconciliation.
     * 
     * @param connRepo Repository connection
     * @param connSource Source connection
     * @param connTarget Target connection
     * @param dct Table information
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     */
    private static void validateReconcileInputs(Connection connRepo, Connection connSource, Connection connTarget,
                                                DataComparisonTable dct, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget) {
        if (connRepo == null) {
            throw new IllegalArgumentException("Repository connection cannot be null");
        }
        if (connSource == null) {
            throw new IllegalArgumentException("Source connection cannot be null");
        }
        if (connTarget == null) {
            throw new IllegalArgumentException("Target connection cannot be null");
        }
        if (dct == null) {
            throw new IllegalArgumentException("Table information cannot be null");
        }
        if (dctmSource == null) {
            throw new IllegalArgumentException("Source table map cannot be null");
        }
        if (dctmTarget == null) {
            throw new IllegalArgumentException("Target table map cannot be null");
        }
    }
    
    /**
     * Create error result object.
     * 
     * @param tableAlias Table alias
     * @param errorMessage Error message
     * @return Error result object
     */
    private static JSONObject createErrorResult(String tableAlias, String errorMessage) {
        JSONObject result = new JSONObject();
        result.put("tableName", tableAlias);
        result.put("status", "failed");
        result.put("compareStatus", "failed");
        result.put("error", errorMessage);
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);
        return result;
    }

    
    /**
     * Perform database comparison operation using optimized services.
     * This method handles both regular comparison and recheck operations.
     * 
     * @param context Application context
     */
    public static void performCompare(ApplicationContext context) {
        boolean isCheck = Props.getProperty("isCheck").equals("true");
        String tableFilter = context.getCmd().hasOption("table") ? context.getCmd().getOptionValue("table").toLowerCase() : "";
        
        LoggingUtils.write("info", THREAD_NAME, String.format("Recheck Out of Sync: %s", isCheck));

        try {
            // Validate context
            validatePerformCompareInputs(context);
            
            // Get tables to process
            RepoController repoController = new RepoController();
            CachedRowSet tablesResultSet = repoController.getTables(context.getPid(), context.getConnRepo(), context.getBatchParameter(), tableFilter, isCheck);
            
            // Process tables and collect results
            TableController.ComparisonResults results = TableController.processTables(tablesResultSet, isCheck, repoController, context);
            
            // Close result set
            if (tablesResultSet != null) {
                tablesResultSet.close();
            }
            
            // Generate summary and reports
            ReportController.createSummary(context, results.tablesProcessed(), results.runResults(), isCheck);
            
            LoggingUtils.write("info", THREAD_NAME, "Comparison operation completed successfully");
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error performing data reconciliation: %s", e.getMessage()));
            throw new RuntimeException("Comparison operation failed", e);
        }
    }
    
    /**
     * Validate inputs for performCompare method.
     * 
     * @param context Application context
     */
    private static void validatePerformCompareInputs(ApplicationContext context) {
        if (context == null) {
            throw new IllegalArgumentException("Application context cannot be null");
        }
        if (context.getConnRepo() == null) {
            throw new IllegalArgumentException("Repository connection cannot be null");
        }
        if (context.getConnSource() == null) {
            throw new IllegalArgumentException("Source connection cannot be null");
        }
        if (context.getConnTarget() == null) {
            throw new IllegalArgumentException("Target connection cannot be null");
        }
    }

}
