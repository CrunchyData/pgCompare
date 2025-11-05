package com.crunchydata.controller;

import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.service.ColumnDiscoveryService;
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.config.sql.DB2SQLConstants.SQL_DB2_SELECT_TABLE;
import static com.crunchydata.config.sql.DB2SQLConstants.SQL_DB2_SELECT_TABLES;
import static com.crunchydata.config.sql.MSSQLSQLConstants.SQL_MSSQL_SELECT_TABLE;
import static com.crunchydata.config.sql.MSSQLSQLConstants.SQL_MSSQL_SELECT_TABLES;
import static com.crunchydata.config.sql.MYSQLSQLConstants.SQL_MYSQL_SELECT_TABLE;
import static com.crunchydata.config.sql.MYSQLSQLConstants.SQL_MYSQL_SELECT_TABLES;
import static com.crunchydata.config.sql.MariaDBSQLConstants.SQL_MARIADB_SELECT_TABLE;
import static com.crunchydata.config.sql.MariaDBSQLConstants.SQL_MARIADB_SELECT_TABLES;
import static com.crunchydata.config.sql.OracleSQLConstants.SQL_ORACLE_SELECT_TABLE;
import static com.crunchydata.config.sql.OracleSQLConstants.SQL_ORACLE_SELECT_TABLES;
import static com.crunchydata.config.sql.PostgresSQLConstants.SQL_POSTGRES_SELECT_TABLE;
import static com.crunchydata.config.sql.PostgresSQLConstants.SQL_POSTGRES_SELECT_TABLES;
import static com.crunchydata.config.sql.RepoSQLConstants.*;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCRESULT_CLEAN;
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCTARGET_CLEAN;
import static com.crunchydata.config.sql.SnowflakeSQLConstants.SQL_SNOWFLAKE_SELECT_TABLE;
import static com.crunchydata.config.sql.SnowflakeSQLConstants.SQL_SNOWFLAKE_SELECT_TABLES;
import static com.crunchydata.service.DatabaseMetadataService.getNativeCase;
import static com.crunchydata.service.DatabaseMetadataService.getTables;
import static com.crunchydata.util.DataProcessingUtils.preserveCase;

public class DiscoverController {

    private static final String THREAD_NAME = "discover-ctrl";


    /**
     * Discover columns from source and target databases using the optimized ColumnDiscoveryService.
     *
     * @param props            Properties for application settings
     * @param pid              Project id
     * @param table            Table to discover columns for
     * @param connRepo         Connection to repository
     * @param connSource       Connection to source database
     * @param connTarget       Connection to target database
     */
    public static void discoverColumns(Properties props, Integer pid, String table,
                                       Connection connRepo, Connection connSource, Connection connTarget) {
        try {
            // Use the optimized discovery service
            ColumnDiscoveryService.discoverColumns(props, pid, table, connRepo, connSource, connTarget);

            LoggingUtils.write("info", THREAD_NAME,
                    String.format("Successfully completed column discovery for project %d", pid));

        } catch (SQLException e) {
            LoggingUtils.write("severe", THREAD_NAME,
                    String.format("Database error during column discovery for project %d: %s", pid, e.getMessage()));
            throw new RuntimeException("Column discovery failed", e);
        } catch (Exception e) {
            LoggingUtils.write("severe", THREAD_NAME,
                    String.format("Unexpected error during column discovery for project %d: %s", pid, e.getMessage()));
            throw new RuntimeException("Column discovery failed", e);
        }
    }

    /**
     * Discover Tables in Specified Schema.
     *
     * @param Props           Properties configuration
     * @param pid             Project ID
     * @param table           Table name filter
     * @param connRepo        Repository database connection
     * @param connSource      Source database connection
     * @param connTarget      Target database connection
     */
    public static void discoverTables(Properties Props, Integer pid, String table, Connection connRepo, Connection connSource, Connection connTarget) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0,pid);

        if (! table.isEmpty()) {
            binds.add(1,table);
        }

        String sql = (table.isEmpty()) ? SQL_REPO_DCTABLE_DELETEBYPROJECT : SQL_REPO_DCTABLE_DELETEBYPROJECTTABLE;

        // Clean previous Discovery
        cleanupPreviousDiscovery(connRepo, sql, binds);

        // Target Table Discovery
        discoverTables(Props, pid, table, connRepo, connTarget, "target",true);

        // Source Table Discovery
        discoverTables(Props, pid, table, connRepo, connSource, "source",false);

        // Clear Incomplete Map
        clearIncompleteMappings(connRepo, pid);
    }

    /**
     * Clean up previous discovery data and orphaned tables.
     *
     * @param connRepo Repository connection
     * @param sql SQL statement for cleanup
     * @param binds Bind parameters
     */
    private static void cleanupPreviousDiscovery(Connection connRepo, String sql, ArrayList<Object> binds) {
        LoggingUtils.write("info", THREAD_NAME, "Clearing previous discovery");
        SQLExecutionHelper.simpleUpdate(connRepo, sql, binds, true);

        // Clean up orphaned tables
        binds.clear();
        SQLExecutionHelper.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_CLEAN, binds, true);
        SQLExecutionHelper.simpleUpdate(connRepo, SQL_REPO_DCTARGET_CLEAN, binds, true);
        SQLExecutionHelper.simpleUpdate(connRepo, SQL_REPO_DCRESULT_CLEAN, binds, true);
        RepoController.vacuumRepo(connRepo);
    }

    /**
     * Clear incomplete table mappings.
     *
     * @param connRepo Repository connection
     * @param pid Project ID
     */
    private static void clearIncompleteMappings(Connection connRepo, Integer pid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.addFirst(pid);
        CachedRowSet crs = SQLExecutionHelper.simpleSelect(connRepo, SQL_REPO_DCTABLE_INCOMPLETEMAP, binds);

        try {
            while (crs.next()) {
                binds.clear();
                binds.addFirst(crs.getInt("tid"));

                LoggingUtils.write("warning", THREAD_NAME, String.format("Skipping table %s due to incomplete mapping (missing source or target)", crs.getString("table_alias")));

                SQLExecutionHelper.simpleUpdate(connRepo, SQL_REPO_DCTABLE_DELETEBYTID, binds, true);
            }

            crs.close();
        } catch (Exception e) {
            LoggingUtils.write("warning", THREAD_NAME, String.format("Error clearing incomplete map: %s", e.getMessage()));
        }
    }

    public static void discoverTables(Properties Props, Integer pid, String table, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTable) {
        String platform=Props.getProperty(destRole+"-type");
        String schema=Props.getProperty(destRole+"-schema");
        ArrayList<Object> binds = new ArrayList<>();
        Integer tableCount = 0;

        LoggingUtils.write("info", THREAD_NAME, String.format("(%s) Performing table discovery on %s for schema %s",destRole, platform,schema));

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

            Integer tid = SQLExecutionHelper.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLE_SELECT_BYNAME, binds);

            DataComparisonTable dct = new DataComparisonTable();
            dct.setPid(pid);
            dct.setTableAlias(tableName.toLowerCase());

            if (tid == null ) {
                if ( populateDCTable ) {
                    dct = RepoController.saveTable(connRepo, dct);
                } else {
                    LoggingUtils.write("warning", THREAD_NAME, String.format("(%s) Skipping, table %s not found on other destination", destRole, tableName));
                }
            } else {
                dct.setTid(tid);
            }

            if ( dct.getTid() != null ) {
                tableCount++;
                DataComparisonTableMap dctm = new DataComparisonTableMap();
                dctm.setTid(dct.getTid());
                dctm.setDestType(destRole);
                dctm.setSchemaName(schemaName);
                dctm.setSchemaPreserveCase(preserveCase(nativeCase, schemaName));
                dctm.setTableName(tableName);
                dctm.setTablePreserveCase(preserveCase(nativeCase, tableName));

                RepoController.saveTableMap(connRepo, dctm);

                LoggingUtils.write("info", THREAD_NAME, String.format("(%s) Discovered Table: %s",destRole, tableName));

            }
        }

        LoggingUtils.write("info", THREAD_NAME, String.format("(%s) Discovered %d tables on %s for for schema %s", destRole, tableCount, platform, schema));

    }

    public static JSONArray getDatabaseTables (String databasePlatform, Connection conn, String schema, String table) {
        return switch (databasePlatform) {
            case "oracle" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_ORACLE_SELECT_TABLES : SQL_ORACLE_SELECT_TABLE );
            case "mariadb" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MARIADB_SELECT_TABLES : SQL_MARIADB_SELECT_TABLE);
            case "mysql" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MYSQL_SELECT_TABLES : SQL_MYSQL_SELECT_TABLE);
            case "mssql" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_MSSQL_SELECT_TABLES : SQL_MSSQL_SELECT_TABLE);
            case "db2" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_DB2_SELECT_TABLES : SQL_DB2_SELECT_TABLE);
            case "snowflake" -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_SNOWFLAKE_SELECT_TABLES : SQL_SNOWFLAKE_SELECT_TABLE);
            default -> getTables(conn, schema, table, (table.isEmpty()) ? SQL_POSTGRES_SELECT_TABLES : SQL_POSTGRES_SELECT_TABLE);
        };

    }



}
