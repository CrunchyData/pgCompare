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
import com.crunchydata.core.comparison.ResultProcessor;
import com.crunchydata.core.threading.DataValidationThread;
import com.crunchydata.core.threading.ThreadManager;
import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.Settings.Props;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID;
import static com.crunchydata.controller.ColumnController.getColumnInfo;
import static com.crunchydata.controller.RepoController.createCompareId;
import static com.crunchydata.service.SQLSyntaxService.buildGetTablesSQL;
import static com.crunchydata.service.SQLSyntaxService.generateCompareSQL;

import org.json.JSONObject;

/**
 * CompareController class that provides a simplified interface for data reconciliation.
 *
 * @author Brian Pace
 */
public class CompareController {

    private static final String THREAD_NAME = "compare-ctrl";


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
            CachedRowSet tablesResultSet = getTables(context.getPid(), context.getConnRepo(), context.getBatchParameter(), tableFilter, isCheck);

            // Process tables and collect results
            TableController.ComparisonResults results = TableController.reconcileTables(tablesResultSet, isCheck, repoController, context);

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
            ArrayList<Object> binds = new ArrayList<>();
            binds.add(dct.getTid());
            String columnMapping = SQLExecutionHelper.simpleSelectReturnString(connRepo, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);

            // Perform preflight checks
            if (!performPreflightChecks(dct, dctmSource, dctmTarget, columnMapping)) {
                return createFailedResult(result);
            }

            // Get column metadata
            JSONObject columnMap = new JSONObject(columnMapping);
            ColumnMetadata ciSource = getColumnInfo(columnMap, "source", Props.getProperty("source-type"),
                    dctmSource.getSchemaName(), dctmSource.getTableName(),
                    "database".equals(Props.getProperty("column-hash-method")));

            ColumnMetadata ciTarget = getColumnInfo(columnMap, "target", Props.getProperty("target-type"),
                    dctmTarget.getSchemaName(), dctmTarget.getTableName(),
                    !check && "database".equals(Props.getProperty("column-hash-method")));




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
     * Retrieve table information from the database.
     *
     * @param pid Project ID
     * @param conn Database connection
     * @param batchNbr Batch number filter
     * @param table Table name filter
     * @param check Check flag for filtering results
     * @return CachedRowSet containing table information
     */
    private static CachedRowSet getTables(Integer pid, Connection conn, Integer batchNbr, String table, Boolean check) {

        String sql = buildGetTablesSQL(batchNbr, table, check);
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(pid);

        if (batchNbr > 0) {
            binds.add(batchNbr);
        }

        if (!table.isEmpty()) {
            binds.add(table);
        }

        LoggingUtils.write("info", THREAD_NAME,
                String.format("Retrieving tables for project %d, batch %d, table filter: %s",
                        pid, batchNbr, table));

        return SQLExecutionHelper.simpleSelect(conn, sql, binds);
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
        SQLExecutionHelper.simpleUpdate(connRepo,
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
