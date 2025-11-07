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
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

public class TableController {

    private static final String THREAD_NAME = "table-ctrl";
    
    // Connection type constants
    private static final String CONN_TYPE_SOURCE = "source";
    private static final String CONN_TYPE_TARGET = "target";
    
    // Status constants
    private static final String STATUS_SKIPPED = "skipped";
    private static final String STATUS_DISABLED = "disabled";
    private static final String STATUS_ERROR = "error";
    private static final String STATUS_FAILED = "failed";


    public static DataComparisonTableMap getTableMap (Connection conn, Integer tid, String tableOrigin) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tid);
        binds.add(1,tableOrigin);

        DataComparisonTableMap result = new DataComparisonTableMap();

        try {

            CachedRowSet crs = SQLExecutionHelper.simpleSelect(conn, SQL_REPO_DCTABLEMAP_SELECTBYTIDORIGIN, binds);

            while (crs.next()) {
                result.setTid(crs.getInt("tid"));
                result.setDestType(crs.getString("dest_type"));
                result.setSchemaName(crs.getString("schema_name"));
                result.setTableName(crs.getString("table_name"));
                result.setModColumn(crs.getString("mod_column"));
                result.setTableFilter(crs.getString("table_filter"));
                result.setSchemaPreserveCase(crs.getBoolean("schema_preserve_case"));
                result.setTablePreserveCase(crs.getBoolean("table_preserve_case"));
            }

        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error retrieving table mapping for tid %d:  %s", tid, e.getMessage()));
            return result;
        }

        return result;

    }

    /**
     * Perform copy table operation.
     * 
     * @param context Application context
     * @return New table ID
     */
    public static int performCopyTable(ApplicationContext context) {
        int newTID = 0;
        String tableName = context.getCmd().getOptionValue("table");
        String newTableName = tableName + "_copy";

        LoggingUtils.write("info", THREAD_NAME, String.format("Copying table and column map for %s to %s", tableName, newTableName));

        ArrayList<Object> binds = new ArrayList<>();
        binds.addFirst(tableName);

        Integer tid = SQLExecutionHelper.simpleSelectReturnInteger(context.getConnRepo(), SQL_REPO_DCTABLE_SELECT_BYNAME, binds);

        binds.clear();
        binds.add(0, context.getPid());
        binds.add(1, tid);
        binds.add(2, newTableName);

        newTID = SQLExecutionHelper.simpleUpdateReturningInteger(context.getConnRepo(), SQL_REPO_DC_COPY_TABLE, binds);

        return newTID;
    }
    
    /**
     * Process all tables in the result set.
     * 
     * @param tablesResultSet Result set containing tables to process
     * @param isCheck Whether this is a recheck operation
     * @param repoController Repository controller instance
     * @return ComparisonResults containing processed tables and results
     * @throws SQLException if database operations fail
     */
    public static ComparisonResults reconcileTables(CachedRowSet tablesResultSet, boolean isCheck, RepoController repoController, ApplicationContext context) throws SQLException {

        JSONArray runResults = new JSONArray();
        int tablesProcessed = 0;
        
        while (tablesResultSet.next()) {
            tablesProcessed++;
            
            // Create DCTable object from result set
            DataComparisonTable table = createDCTableFromResultSet(tablesResultSet, context.getPid());

            JSONObject actionResult;

            if (table.getEnabled()) {
                actionResult = reconcileEnabledTable(table, isCheck, repoController, context);
            } else {
                actionResult = createSkippedTableResult(table);
            }

            runResults.put(actionResult);
        }
        
        return new ComparisonResults(tablesProcessed, runResults);
    }
    
    /**
     * Create a DCTable object from the result set.
     * 
     * @param resultSet The result set containing table data
     * @param pid Project ID
     * @return DCTable object
     * @throws SQLException if database operations fail
     */
    public static DataComparisonTable createDCTableFromResultSet(CachedRowSet resultSet, Integer pid) throws SQLException {
        DataComparisonTable dct = new DataComparisonTable();
        dct.setPid(pid);
        dct.setTid(resultSet.getInt("tid"));
        dct.setEnabled(resultSet.getBoolean("enabled"));
        dct.setBatchNbr(resultSet.getInt("batch_nbr"));
        dct.setParallelDegree(resultSet.getInt("parallel_degree"));
        dct.setTableAlias(resultSet.getString("table_alias"));
        return dct;
    }

    /**
     * Perform reconcilation an enabled table for comparison.
     * 
     * @param table The table to process
     * @param isCheck Whether this is a recheck operation
     * @param repoController Repository controller instance
     * @param context Application context
     * @return JSONObject containing the result of processing this table
     */
    public static JSONObject reconcileEnabledTable(DataComparisonTable table, boolean isCheck, RepoController repoController, ApplicationContext context) {
        LoggingUtils.write("info", THREAD_NAME, String.format("--- START RECONCILIATION FOR TABLE: %s ---",
            table.getTableAlias().toUpperCase()));

        try {
            // Create table maps for source and target
            DataComparisonTableMap sourceTableMap = createTableMap(context.getConnRepo(), table.getTid(), CONN_TYPE_SOURCE);
            DataComparisonTableMap targetTableMap = createTableMap(context.getConnRepo(), table.getTid(), CONN_TYPE_TARGET);
            
            // Set batch number and project ID
            sourceTableMap.setBatchNbr(table.getBatchNbr());
            sourceTableMap.setPid(context.getPid());
            sourceTableMap.setTableAlias(table.getTableAlias());
            
            targetTableMap.setBatchNbr(table.getBatchNbr());
            targetTableMap.setPid(context.getPid());
            targetTableMap.setTableAlias(table.getTableAlias());

            // Start table history tracking
            repoController.startTableHistory(context.getConnRepo(), table.getTid(), table.getBatchNbr());

            // Clear previous results if not a recheck
            if (!isCheck) {
                LoggingUtils.write("info", THREAD_NAME, "Clearing data compare findings");
                repoController.deleteDataCompare(context.getConnRepo(), table.getTid(), table.getBatchNbr());
            }

            // Perform the actual comparison
            JSONObject actionResult = CompareController.reconcileData(
                context.getConnRepo(), context.getConnSource(), context.getConnTarget(), 
                context.getStartStopWatch(), isCheck, table, sourceTableMap, targetTableMap);

            // Complete table history
            repoController.completeTableHistory(context.getConnRepo(), table.getTid(), table.getBatchNbr(), 0, actionResult.toString());
            
            return actionResult;
            
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME, String.format("Error processing table %s: %s",
                table.getTableAlias(), e.getMessage()));
            return createErrorTableResult(table, e.getMessage());
        }
    }
    
    /**
     * Create a result object for a skipped (disabled) table.
     * 
     * @param table The table that was skipped
     * @return JSONObject containing skip result
     */
    public static JSONObject createSkippedTableResult(DataComparisonTable table) {
        LoggingUtils.write("warning", THREAD_NAME, String.format("Skipping disabled table: %s",
            table.getTableAlias().toUpperCase()));
        
        JSONObject result = new JSONObject();
        result.put("tableName", table.getTableAlias());
        result.put("status", STATUS_SKIPPED);
        result.put("compareStatus", STATUS_DISABLED);
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);
        return result;
    }
    
    /**
     * Create a result object for a table that encountered an error.
     * 
     * @param table The table that encountered an error
     * @param errorMessage The error message
     * @return JSONObject containing error result
     */
    public static JSONObject createErrorTableResult(DataComparisonTable table, String errorMessage) {
        JSONObject result = new JSONObject();
        result.put("tableName", table.getTableAlias());
        result.put("status", STATUS_ERROR);
        result.put("compareStatus", STATUS_FAILED);
        result.put("error", errorMessage);
        result.put("missingSource", 0);
        result.put("missingTarget", 0);
        result.put("notEqual", 0);
        result.put("equal", 0);
        return result;
    }
    

    /**
     * Inner class to hold comparison results.
    */
    public record ComparisonResults(int tablesProcessed, JSONArray runResults) { }
    
    /**
     * Create a table map for the specified connection type.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param tableOrigin Connection type (source/target)
     * @return DCTableMap object
     */
    public static DataComparisonTableMap createTableMap(Connection connRepo, Integer tid, String tableOrigin) {
        return getTableMap(connRepo, tid, tableOrigin);
    }
}
