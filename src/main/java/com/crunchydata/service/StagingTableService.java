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

import com.crunchydata.core.database.SQLExecutionHelper;
import com.crunchydata.util.LoggingUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;
import static com.crunchydata.config.Settings.Props;

/**
 * Service class for managing staging table operations in the repository.
 * This class encapsulates the logic for staging table creation, management,
 * and data loading operations, providing a clean interface for staging-related operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class StagingTableService {
    
    private static final String THREAD_NAME = "staging-operations";
    
    /**
     * Create a staging table for data comparison.
     * 
     * @param conn Database connection
     * @param location Location identifier (source or target)
     * @param tid Table ID
     * @param threadNbr Thread number
     * @return The name of the created staging table
     * @throws SQLException if database operations fail
     */
    public static String createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr) 
            throws SQLException {

        String sql = String.format(REPO_DDL_STAGE_TABLE, Props.getProperty("stage-table-parallel"));
        String stagingTable = String.format("dc_%s_%s_%s", location, tid, threadNbr);
        
        sql = sql.replaceAll("dc_source", stagingTable);
        
        // Drop existing staging table if it exists
        dropStagingTable(conn, stagingTable);
        
        // Create new staging table
        SQLExecutionHelper.simpleExecute(conn, sql);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Staging table created: %s for location: %s, table: %d, thread: %d", 
                stagingTable, location, tid, threadNbr));
        
        return stagingTable;
    }
    
    /**
     * Drop a staging table if it exists.
     * 
     * @param conn Database connection
     * @param stagingTable Staging table name
     * @throws SQLException if database operations fail
     */
    public static void dropStagingTable(Connection conn, String stagingTable) throws SQLException {

        String sql = String.format("DROP TABLE IF EXISTS %s", stagingTable);
        SQLExecutionHelper.simpleExecute(conn, sql);
        
        LoggingUtils.write("debug", THREAD_NAME,
            String.format("Staging table dropped: %s", stagingTable));
    }
    
    /**
     * Load findings from the staging table into the main table.
     * 
     * @param conn Database connection
     * @param location Location identifier (source or target)
     * @param tid Table ID
     * @param stagingTable Staging table name
     * @param batchNbr Batch number
     * @param threadNbr Thread number
     * @param tableAlias Table alias
     * @throws SQLException if database operations fail
     */
    public static void loadFindings(Connection conn, String location, Integer tid, String stagingTable, 
                                  Integer batchNbr, Integer threadNbr, String tableAlias) throws SQLException {

        String sqlFinal = SQL_REPO_DCSOURCE_INSERT
            .replaceAll("dc_source", String.format("dc_%s", location))
            .replaceAll("stagingtable", stagingTable);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(threadNbr);
        binds.add(batchNbr);
        binds.add(tableAlias);
        
        SQLExecutionHelper.simpleUpdate(conn, sqlFinal, binds, true);
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("Findings loaded from staging table %s to main table for location: %s", 
                stagingTable, location));
    }

}
