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
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.model.DataComparisonTable;
import com.crunchydata.model.DataComparisonTableMap;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;

import java.sql.Connection;
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
import static com.crunchydata.config.sql.RepoSQLConstants.SQL_REPO_DCTABLE_SELECT_BYNAME;
import static com.crunchydata.config.sql.SnowflakeSQLConstants.SQL_SNOWFLAKE_SELECT_TABLE;
import static com.crunchydata.config.sql.SnowflakeSQLConstants.SQL_SNOWFLAKE_SELECT_TABLES;
import static com.crunchydata.service.DatabaseMetadataService.getNativeCase;
import static com.crunchydata.service.DatabaseMetadataService.getTables;
import static com.crunchydata.util.DataProcessingUtils.preserveCase;

public class TableDiscoveryService {

    private static final String THREAD_NAME = "column-discovery";

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
