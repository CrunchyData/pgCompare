package com.crunchydata.controller;

import com.crunchydata.ApplicationContext;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.services.DatabaseService.getNativeCase;
import static com.crunchydata.services.DatabaseService.getTables;
import static com.crunchydata.util.DataUtility.preserveCase;
import static com.crunchydata.util.SQLConstantsDB2.SQL_DB2_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsDB2.SQL_DB2_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsMSSQL.SQL_MSSQL_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsMSSQL.SQL_MSSQL_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsMYSQL.SQL_MYSQL_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsMYSQL.SQL_MYSQL_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsMariaDB.SQL_MARIADB_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsMariaDB.SQL_MARIADB_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsOracle.SQL_ORACLE_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsOracle.SQL_ORACLE_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsPostgres.SQL_POSTGRES_SELECT_TABLE;
import static com.crunchydata.util.SQLConstantsPostgres.SQL_POSTGRES_SELECT_TABLES;
import static com.crunchydata.util.SQLConstantsRepo.*;

public class TableController {

    private static final String THREAD_NAME = "table-ctrl";

    /**
     * Discover Tables in Specified Schema.
     *
     * @param connRepo        Repository database connection
     * @param connSource      Source database connection
     * @param connTarget      Target database connection
     */
    public static void discoverTables (Properties Props, Integer pid, String table, Connection connRepo, Connection connSource, Connection connTarget) {

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,pid);

        if (! table.isEmpty()) {
            binds.add(1,table);
        }

        String sql = (table.isEmpty()) ? SQL_REPO_DCTABLE_DELETEBYPROJECT : SQL_REPO_DCTABLE_DELETEBYPROJECTTABLE;

        // Clean previous Discovery
        Logging.write("info", THREAD_NAME, "Clearing previous discovery");
        SQLService.simpleUpdate(connRepo, sql, binds, true);

        // Clean up orphaned tables
        binds.clear();
        SQLService.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_CLEAN, binds, true);
        SQLService.simpleUpdate(connRepo, SQL_REPO_DCTARGET_CLEAN, binds, true);
        SQLService.simpleUpdate(connRepo, SQL_REPO_DCRESULT_CLEAN, binds, true);
        RepoController.vacuumRepo(connRepo);

        // Target Table Discovery
        loadTables(Props, pid, table, connRepo, connTarget, "target",true);

        // Source Table Discovery
        loadTables(Props, pid, table, connRepo, connSource, "source",false);

        // Clear Incomplete Map
        binds.clear();
        binds.addFirst(pid);
        CachedRowSet crs = SQLService.simpleSelect(connRepo, SQL_REPO_DCTABLE_INCOMPLETEMAP, binds);

        try {
            while (crs.next()) {
                binds.clear();
                binds.addFirst(crs.getInt("tid"));

                Logging.write("warning",THREAD_NAME,String.format("Skipping table %s due to incomplete mapping (missing source or target)",crs.getString("table_alias")));

                SQLService.simpleUpdate(connRepo,SQL_REPO_DCTABLE_DELETEBYTID, binds,true);
            }

            crs.close();
        } catch (Exception e) {
            Logging.write("warning",THREAD_NAME,String.format("Error clearing incomplete map: %s",e.getMessage()));
        }
    }

    public static DCTableMap getTableMap (Connection conn, Integer tid, String tableOrigin) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,tid);
        binds.add(1,tableOrigin);

        DCTableMap result = new DCTableMap();

        try {

            CachedRowSet crs = SQLService.simpleSelect(conn, SQL_REPO_DCTABLEMAP_SELECTBYTIDORIGIN, binds);

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
            Logging.write("severe", THREAD_NAME, String.format("Error retrieving table mapping for tid %d:  %s", tid, e.getMessage()));
            return result;
        }

        return result;

    }

    public static JSONArray getDatabaseTables (String databasePlatform, Connection conn, String schema, String table) {

        return switch (databasePlatform) {
            case "oracle" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_ORACLE_SELECT_TABLES : SQL_ORACLE_SELECT_TABLE );
            case "mariadb" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MARIADB_SELECT_TABLES : SQL_MARIADB_SELECT_TABLE);
            case "mysql" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MYSQL_SELECT_TABLES : SQL_MYSQL_SELECT_TABLE);
            case "mssql" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MSSQL_SELECT_TABLES : SQL_MSSQL_SELECT_TABLE);
            case "db2" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_DB2_SELECT_TABLES : SQL_DB2_SELECT_TABLE);
            default -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_POSTGRES_SELECT_TABLES : SQL_POSTGRES_SELECT_TABLE);
        };

    }

    public static void loadTables(Properties Props, Integer pid, String table, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTable) {
        String platform=Props.getProperty(destRole+"-type");
        String schema=Props.getProperty(destRole+"-schema");
        ArrayList<Object> binds = new ArrayList<>();
        Integer tableCount = 0;

        Logging.write("info", THREAD_NAME, String.format("(%s) Performing table discovery on %s for schema %s",destRole, platform,schema));

        // Get Tables based on Platform
        JSONArray tables = getDatabaseTables(platform,connDest,schema, table);

        // Get Default Case for Platform
        String nativeCase = getNativeCase(platform);

        // Populate dc_table and target table map
        for (int i = 0; i < tables.length(); i++) {
            String schemaName = tables.getJSONObject(i).getString("schemaName");
            String tableName = tables.getJSONObject(i).getString("tableName");

            binds.clear();
            binds.add(0,tableName);
            binds.add(1,pid);

            Integer tid = SQLService.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLE_SELECT_BYNAME, binds);

            DCTable dct = new DCTable();
            dct.setPid(pid);
            dct.setTableAlias(tableName.toLowerCase());

            if (tid == null ) {
                if ( populateDCTable ) {
                    dct = RepoController.saveTable(connRepo, dct);
                } else {
                    Logging.write("warning", THREAD_NAME, String.format("(%s) Skipping, table %s not found on other destination", destRole, tableName));
                }
            } else {
                dct.setTid(tid);
            }

            if ( dct.getTid() != null ) {
                tableCount++;
                DCTableMap dctm = new DCTableMap();
                dctm.setTid(dct.getTid());
                dctm.setDestType(destRole);
                dctm.setSchemaName(schemaName);
                dctm.setSchemaPreserveCase(preserveCase(nativeCase, schemaName));
                dctm.setTableName(tableName);
                dctm.setTablePreserveCase(preserveCase(nativeCase, tableName));

                RepoController.saveTableMap(connRepo, dctm);

                Logging.write("info", THREAD_NAME, String.format("(%s) Discovered Table: %s",destRole, tableName));

            }
        }

        Logging.write("info", THREAD_NAME, String.format("(%s) Discovered %d tables on %s for for schema %s", destRole, tableCount, platform, schema));

    }
    
    // Constants for table processing
    private static final String STATUS_SKIPPED = "skipped";
    private static final String STATUS_DISABLED = "disabled";
    private static final String CONN_TYPE_SOURCE = "source";
    private static final String CONN_TYPE_TARGET = "target";
    
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

        Logging.write("info", THREAD_NAME, String.format("Copying table and column map for %s to %s", tableName, newTableName));

        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, tableName);

        Integer tid = SQLService.simpleSelectReturnInteger(context.getConnRepo(), SQL_REPO_DCTABLE_SELECT_BYNAME, binds);

        binds.clear();
        binds.add(0, context.getPid());
        binds.add(1, tid);
        binds.add(2, newTableName);

        newTID = SQLService.simpleUpdateReturningInteger(context.getConnRepo(), SQL_REPO_DC_COPY_TABLE, binds);

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
    public static ComparisonResults processTables(CachedRowSet tablesResultSet, boolean isCheck, RepoController repoController, ApplicationContext context) throws SQLException {
        JSONArray runResults = new JSONArray();
        int tablesProcessed = 0;
        
        while (tablesResultSet.next()) {
            tablesProcessed++;
            
            // Create DCTable object from result set
            DCTable table = createDCTableFromResultSet(tablesResultSet, context.getPid());
            
            // Process the table and get results
            JSONObject actionResult = processTable(table, isCheck, repoController, context);
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
    public static DCTable createDCTableFromResultSet(CachedRowSet resultSet, Integer pid) throws SQLException {
        DCTable dct = new DCTable();
        dct.setPid(pid);
        dct.setTid(resultSet.getInt("tid"));
        dct.setEnabled(resultSet.getBoolean("enabled"));
        dct.setBatchNbr(resultSet.getInt("batch_nbr"));
        dct.setParallelDegree(resultSet.getInt("parallel_degree"));
        dct.setTableAlias(resultSet.getString("table_alias"));
        return dct;
    }
    
    /**
     * Process a single table for comparison.
     * 
     * @param table The table to process
     * @param isCheck Whether this is a recheck operation
     * @param repoController Repository controller instance
     * @param context Application context
     * @return JSONObject containing the result of processing this table
     */
    public static JSONObject processTable(DCTable table, boolean isCheck, RepoController repoController, ApplicationContext context) {
        if (table.getEnabled()) {
            return processEnabledTable(table, isCheck, repoController, context);
        } else {
            return createSkippedTableResult(table);
        }
    }
    
    /**
     * Process an enabled table for comparison.
     * 
     * @param table The table to process
     * @param isCheck Whether this is a recheck operation
     * @param repoController Repository controller instance
     * @param context Application context
     * @return JSONObject containing the result of processing this table
     */
    public static JSONObject processEnabledTable(DCTable table, boolean isCheck, RepoController repoController, ApplicationContext context) {
        Logging.write("info", THREAD_NAME, String.format("--- START RECONCILIATION FOR TABLE: %s ---", 
            table.getTableAlias().toUpperCase()));

        try {
            // Create table maps for source and target
            DCTableMap sourceTableMap = createTableMap(context.getConnRepo(), table.getTid(), CONN_TYPE_SOURCE);
            DCTableMap targetTableMap = createTableMap(context.getConnRepo(), table.getTid(), CONN_TYPE_TARGET);
            
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
                Logging.write("info", THREAD_NAME, "Clearing data compare findings");
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
            Logging.write("severe", THREAD_NAME, String.format("Error processing table %s: %s", 
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
    public static JSONObject createSkippedTableResult(DCTable table) {
        Logging.write("warning", THREAD_NAME, String.format("Skipping disabled table: %s", 
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
    public static JSONObject createErrorTableResult(DCTable table, String errorMessage) {
        JSONObject result = new JSONObject();
        result.put("tableName", table.getTableAlias());
        result.put("status", "error");
        result.put("compareStatus", "failed");
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
    public static class ComparisonResults {
        private final int tablesProcessed;
        private final JSONArray runResults;
        
        public ComparisonResults(int tablesProcessed, JSONArray runResults) {
            this.tablesProcessed = tablesProcessed;
            this.runResults = runResults;
        }
        
        public int getTablesProcessed() { return tablesProcessed; }
        public JSONArray getRunResults() { return runResults; }
    }
    
    /**
     * Create a table map for the specified connection type.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param tableOrigin Connection type (source/target)
     * @return DCTableMap object
     */
    public static DCTableMap createTableMap(Connection connRepo, Integer tid, String tableOrigin) {
        return getTableMap(connRepo, tid, tableOrigin);
    }
}
