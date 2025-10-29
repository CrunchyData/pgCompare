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

package com.crunchydata.service;

import com.crunchydata.controller.RepoController;
import com.crunchydata.core.database.ResultProcessor;
import com.crunchydata.core.threading.ThreadManager;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.core.threading.DataValidationThread;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.controller.ColumnController.getColumnInfo;
import static com.crunchydata.service.DatabaseMetadataService.buildLoadSQL;
import static com.crunchydata.config.sql.RepoSQLConstants.*;
import static com.crunchydata.config.Settings.Props;

/**
 * Service class for handling data reconciliation operations.
 * This class encapsulates the complex logic of reconciling data between
 * source and target databases, including column mapping, SQL generation,
 * and result processing.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class DataReconciliationService {
    
    private static final String THREAD_NAME = "data-reconciliation";
    
    /**
     * Reconcile data between source and target databases.
     * 
     * @param connRepo Repository connection
     * @param connSource Source database connection
     * @param connTarget Target database connection
     * @param rid Reconciliation ID
     * @param check Whether to perform a check operation
     * @param dct Table information
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @return JSON object with reconciliation results
     */
    public static JSONObject reconcileData(Connection connRepo, Connection connSource, Connection connTarget,
                                           long rid, Boolean check, DataComparisonTable dct, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget) {
        
        long startTime = System.currentTimeMillis();
        JSONObject result = initializeResult(dct);
        
        try {
            // Get column mapping
            String columnMapping = getColumnMapping(connRepo, dct.getTid());
            
            // Perform preflight checks
            if (!performPreflightChecks(dct, dctmSource, dctmTarget, columnMapping)) {
                return createFailedResult(result);
            }
            
            // Get column metadata
            JSONObject columnMap = new JSONObject(columnMapping);
            ColumnMetadata ciSource = getSourceColumnMetadata(columnMap, dctmSource);
            ColumnMetadata ciTarget = getTargetColumnMetadata(columnMap, dctmTarget, check);
            
            logColumnMetadata(ciSource, ciTarget);
            
            // Create compare ID
            Integer cid = createCompareId(connRepo, dctmTarget, rid);
            
            // Generate compare SQL
            generateCompareSQL(dctmSource, dctmTarget, ciSource, ciTarget);
            
            // Execute reconciliation
            if (check) {
                executeRecheck(connRepo, connSource, connTarget, dct, dctmSource, dctmTarget, ciSource, ciTarget, cid, result);
            } else {
                executeReconciliation(connRepo, dct, cid, dctmSource, dctmTarget, ciSource, ciTarget, result);
            }
            
            // Process results
            processResults(connRepo, dct.getTid(), result, cid, startTime);
            
            result.put("status", "success");
            
        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Database error during reconciliation: %s", e.getMessage()));
            result.put("status", "failed");
            result.put("compareStatus", "failed");
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Unexpected error during reconciliation: %s", e.getMessage()));
            result.put("status", "failed");
            result.put("compareStatus", "failed");
        }
        
        return result;
    }
    
    /**
     * Get column mapping for the table.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @return Column mapping JSON string
     * @throws SQLException if database operations fail
     */
    private static String getColumnMapping(Connection connRepo, Integer tid) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        return SQLExecutionService.simpleSelectReturnString(connRepo, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);
    }
    
    /**
     * Perform preflight checks before reconciliation.
     * 
     * @param dct Table information
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param columnMapping Column mapping string
     * @return true if checks pass, false otherwise
     */
    private static boolean performPreflightChecks(DataComparisonTable dct, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget, String columnMapping) {
        // Check parallel degree and mod column
        if (dct.getParallelDegree() > 1 && dctmSource.getModColumn().isEmpty() && dctmTarget.getModColumn().isEmpty()) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("Parallel degree is greater than 1 for table %s, but no value specified for mod_column on source and/or target.", 
                    dct.getTableAlias()));
            return false;
        }
        
        // Verify column mapping exists
        if (columnMapping == null) {
            LoggingUtils.write("severe", THREAD_NAME,
                String.format("No column map found for table %s. Consider running with maponly option to create mappings.", 
                    dct.getTableAlias()));
            return false;
        }
        
        return true;
    }
    
    /**
     * Get source column metadata.
     * 
     * @param columnMap Column mapping JSON
     * @param dctmSource Source table map
     * @return Column metadata for source
     */
    private static ColumnMetadata getSourceColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmSource) {
        return getColumnInfo(columnMap, "source", Props.getProperty("source-type"), 
            dctmSource.getSchemaName(), dctmSource.getTableName(), 
            "database".equals(Props.getProperty("column-hash-method")));
    }
    
    /**
     * Get target column metadata.
     * 
     * @param columnMap Column mapping JSON
     * @param dctmTarget Target table map
     * @param check Whether this is a check operation
     * @return Column metadata for target
     */
    private static ColumnMetadata getTargetColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmTarget, Boolean check) {
        return getColumnInfo(columnMap, "target", Props.getProperty("target-type"), 
            dctmTarget.getSchemaName(), dctmTarget.getTableName(), 
            !check && "database".equals(Props.getProperty("column-hash-method")));
    }
    
    /**
     * Create compare ID for this reconciliation run.
     * 
     * @param connRepo Repository connection
     * @param dctmTarget Target table map
     * @param rid Reconciliation ID
     * @return Compare ID
     * @throws SQLException if database operations fail
     */
    private static Integer createCompareId(Connection connRepo, DataComparisonTableMap dctmTarget, long rid) throws SQLException {
        RepoController rpc = new RepoController();
        return rpc.dcrCreate(connRepo, dctmTarget.getTid(), dctmTarget.getTableAlias(), rid);
    }
    
    /**
     * Generate compare SQL for source and target.
     * 
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     */
    private static void generateCompareSQL(DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                           ColumnMetadata ciSource, ColumnMetadata ciTarget) {
        String method = Props.getProperty("column-hash-method");
        dctmSource.setCompareSQL(buildLoadSQL(method, dctmSource, ciSource));
        dctmTarget.setCompareSQL(buildLoadSQL(method, dctmTarget, ciTarget));
        
        LoggingUtils.write("info", THREAD_NAME, "(source) Compare SQL: " + dctmSource.getCompareSQL());
        LoggingUtils.write("info", THREAD_NAME, "(target) Compare SQL: " + dctmTarget.getCompareSQL());
    }
    
    /**
     * Execute recheck operation.
     * 
     * @param connRepo Repository connection
     * @param connSource Source connection
     * @param connTarget Target connection
     * @param dct Table information
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     * @param cid Compare ID
     * @param result Result object to update
     * @throws SQLException if database operations fail
     */
    private static void executeRecheck(Connection connRepo, Connection connSource, Connection connTarget,
                                       DataComparisonTable dct, DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                       ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer cid, JSONObject result)
                                     throws SQLException {
        JSONObject checkResult = DataValidationThread.checkRows(connRepo, connSource, connTarget, dct, dctmSource, dctmTarget, ciSource, ciTarget, cid);
        result.put("checkResult", checkResult);
    }
    
    /**
     * Execute reconciliation operation.
     * 
     * @param connRepo Repository connection
     * @param dct Table information
     * @param cid Compare ID
     * @param dctmSource Source table map
     * @param dctmTarget Target table map
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     * @param result Result object to update
     * @throws SQLException if database operations fail
     */
    private static void executeReconciliation(Connection connRepo, DataComparisonTable dct, Integer cid,
                                              DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                                              ColumnMetadata ciSource, ColumnMetadata ciTarget, JSONObject result)
                                            throws SQLException {
        if (hasNoPrimaryKeys(ciSource, ciTarget)) {
            skipReconciliation(connRepo, result, dctmTarget.getTableName(), cid);
        } else {
            try {
                // Use ThreadManager for complex thread coordination
                ThreadManager.executeReconciliation(dct, cid, dctmSource, dctmTarget, ciSource, ciTarget, connRepo);
            } catch (InterruptedException e) {
                LoggingUtils.write("severe", THREAD_NAME, String.format("Thread execution interrupted: %s", e.getMessage()));
                Thread.currentThread().interrupt();
                throw new SQLException("Thread execution interrupted", e);
            }
        }
    }
    
    /**
     * Check if tables have no primary keys.
     * 
     * @param ciSource Source column metadata
     * @param ciTarget Target column metadata
     * @return true if no primary keys found
     */
    private static boolean hasNoPrimaryKeys(ColumnMetadata ciSource, ColumnMetadata ciTarget) {
        return ciTarget.pkList.isBlank() || ciTarget.pkList.isEmpty() || 
               ciSource.pkList.isBlank() || ciSource.pkList.isEmpty();
    }
    
    /**
     * Skip reconciliation for tables without primary keys.
     * 
     * @param connRepo Repository connection
     * @param result Result object to update
     * @param tableName Table name
     * @param cid Compare ID
     */
    private static void skipReconciliation(Connection connRepo, JSONObject result, String tableName, Integer cid)  {
        LoggingUtils.write("warning", THREAD_NAME,
            String.format("Table %s has no Primary Key, skipping reconciliation", tableName));
        result.put("status", "skipped");
        result.put("compareStatus", "skipped");
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(cid);
        SQLExecutionService.simpleUpdate(connRepo,
            "UPDATE dc_result SET equal_cnt=0,missing_source_cnt=0,missing_target_cnt=0,not_equal_cnt=0,source_cnt=0,target_cnt=0,status='skipped' WHERE cid=?", 
            binds, true);
    }
    
    /**
     * Process reconciliation results.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param result Result object to update
     * @param cid Compare ID
     * @param startTime Start time of operation
     * @throws SQLException if database operations fail
     */
    private static void processResults(Connection connRepo, long tid, JSONObject result, Integer cid, long startTime) 
            throws SQLException {
        ResultProcessor.summarizeResults(connRepo, tid, result, cid);
        
        long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
        result.put("elapsedTime", elapsedTime);
        result.put("rowsPerSecond", (elapsedTime > 0) ? result.getInt("totalRows") / elapsedTime : result.getInt("totalRows"));
        
        logFinalResult(result, result.getString("tableName"));
    }
    
    /**
     * Initialize result object with default values.
     * 
     * @param dct Table information
     * @return Initialized result object
     */
    private static JSONObject initializeResult(DataComparisonTable dct) {
        JSONObject result = new JSONObject();
        result.put("tableName", dct.getTableAlias());
        result.put("status", "processing");
        result.put("compareStatus", "processing");
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);
        return result;
    }
    
    /**
     * Create failed result object.
     * 
     * @param result Base result object
     * @return Failed result object
     */
    private static JSONObject createFailedResult(JSONObject result) {
        result.put("status", "failed");
        result.put("compareStatus", "failed");
        return result;
    }
    
    /**
     * Log column metadata information.
     * 
     * @param source Source column metadata
     * @param target Target column metadata
     */
    private static void logColumnMetadata(ColumnMetadata source, ColumnMetadata target) {
        LoggingUtils.write("info", THREAD_NAME, "(source) Columns: " + source.columnList);
        LoggingUtils.write("info", THREAD_NAME, "(target) Columns: " + target.columnList);
        LoggingUtils.write("info", THREAD_NAME, "(source) PK Columns: " + source.pkList);
        LoggingUtils.write("info", THREAD_NAME, "(target) PK Columns: " + target.pkList);
    }
    
    /**
     * Log final reconciliation result.
     * 
     * @param result Result object
     * @param tableAlias Table alias
     */
    private static void logFinalResult(JSONObject result, String tableAlias) {
        java.text.DecimalFormat formatter = new java.text.DecimalFormat("#,###");
        LoggingUtils.write("info", THREAD_NAME, String.format(
            "Reconciliation Complete: Table = %s; Status = %s; Equal = %s; Not Equal = %s; Missing Source = %s; Missing Target = %s",
            tableAlias, result.getString("compareStatus"),
            formatter.format(result.getInt("equal")),
            formatter.format(result.getInt("notEqual")),
            formatter.format(result.getInt("missingSource")),
            formatter.format(result.getInt("missingTarget"))
        ));
    }
}
