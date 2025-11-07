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

import com.crunchydata.service.ColumnDiscoveryService;
import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.config.sql.RepoSQLConstants.*;
import static com.crunchydata.service.TableDiscoveryService.discoverTables;

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
    public static void performColumnDiscovery(Properties props, Integer pid, String table,
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
    public static void performTableDiscovery(Properties Props, Integer pid, String table, Connection connRepo, Connection connSource, Connection connTarget) {
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


}
