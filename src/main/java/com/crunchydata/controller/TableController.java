package com.crunchydata.controller;

import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import org.json.JSONArray;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.util.DataUtility.getNativeCase;
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

    private static final String THREAD_NAME = "DiscoveryController";

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

        //
        // Clean previous Discovery
        //
        Logging.write("info", THREAD_NAME, "Clearing previous discovery");
        dbCommon.simpleUpdate(connRepo, sql, binds, true);

        // Clean up orphaned tables
        binds.clear();
        dbCommon.simpleUpdate(connRepo, SQL_REPO_DCSOURCE_CLEAN, binds, true);
        dbCommon.simpleUpdate(connRepo, SQL_REPO_DCTARGET_CLEAN, binds, true);
        dbCommon.simpleUpdate(connRepo, SQL_REPO_DCRESULT_CLEAN, binds, true);
        RepoController.vacuumRepo(connRepo);


        // Target Table Discovery
        loadTables(Props, pid, table, connRepo, connTarget, "target",true);

        // Source Table Discovery
        loadTables(Props, pid, table, connRepo, connSource, "source",false);

        // Clear Incomplete Map
        binds.clear();
        binds.addFirst(pid);
        CachedRowSet crs = dbCommon.simpleSelect(connRepo, SQL_REPO_DCTABLE_INCOMPLETEMAP, binds);

        try {
            while (crs.next()) {
                binds.clear();
                binds.addFirst(crs.getInt("tid"));

                Logging.write("warning",THREAD_NAME,String.format("Skipping table %s due to incomplete mapping (missing source or target)",crs.getString("table_alias")));
                dbCommon.simpleUpdate(connRepo,SQL_REPO_DCTABLE_DELETEBYTID, binds,true);
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

            CachedRowSet crs = dbCommon.simpleSelect(conn, SQL_REPO_DCTABLEMAP_SELECTBYTIDORIGIN, binds);

            while (crs.next()) {
                result.setTid(crs.getInt("tid"));
                result.setDestType(crs.getString("dest_type"));
                result.setSchemaName(crs.getString("schema_name"));
                result.setTableName(crs.getString("table_name"));
                result.setParallelDegree(crs.getInt("parallel_degree"));
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
            case "oracle" -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_ORACLE_SELECT_TABLES : SQL_ORACLE_SELECT_TABLE );
            case "mariadb" -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_MARIADB_SELECT_TABLES : SQL_MARIADB_SELECT_TABLE);
            case "mysql" -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_MYSQL_SELECT_TABLES : SQL_MYSQL_SELECT_TABLE);
            case "mssql" -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_MSSQL_SELECT_TABLES : SQL_MSSQL_SELECT_TABLE);
            case "db2" -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_DB2_SELECT_TABLES : SQL_DB2_SELECT_TABLE);
            default -> dbCommon.getTables(conn, schema, table, (table.isEmpty()) ? SQL_POSTGRES_SELECT_TABLES : SQL_POSTGRES_SELECT_TABLE);
        };
    }


    public static void loadTables(Properties Props, Integer pid, String table, Connection connRepo, Connection connDest, String destRole, Boolean populateDCTable) {
        String destType=Props.getProperty(destRole+"-type");
        String schema=Props.getProperty(destRole+"-schema");
        ArrayList<Object> binds = new ArrayList<>();
        Integer tableCount = 0;

        Logging.write("info", THREAD_NAME, String.format("(%s) Performing table discovery on %s for schema %s",destRole, destType,schema));

        // Get Tables based on Platform
        JSONArray tables = getDatabaseTables(destType,connDest,schema, table);

        // Get Default Case for Platform
        String nativeCase = getNativeCase(destType);

        // Populate dc_table and target table map
        for (int i = 0; i < tables.length(); i++) {
            String schemaName = tables.getJSONObject(i).getString("schemaName");
            String tableName = tables.getJSONObject(i).getString("tableName");

            binds.clear();
            binds.add(0,tableName);
            binds.add(1,pid);

            Integer tid = dbCommon.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLE_SELECT_BYNAME, binds);

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

        Logging.write("info", THREAD_NAME, String.format("(%s) Discovered %d tables on %s for for schema %s", destRole, tableCount, destType, schema));

    }
}
